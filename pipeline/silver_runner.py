import glob as glob_mod
import json
import os
import subprocess
from datetime import datetime, timezone

import duckdb

from pipeline.run_log import sanitise_error, write_run_log_row

SILVER_TC_PATH = 'data/silver/transaction_codes/data.parquet'


def run_silver_transaction_codes(run_id: str) -> str:
    if os.path.exists(SILVER_TC_PATH):
        write_run_log_row(run_id, 'silver_transaction_codes', 'SILVER', status='SKIPPED')
        return 'SKIPPED'
    started_at = datetime.now(timezone.utc)
    result = subprocess.run(
        ['dbt', 'run', '--select', 'silver_transaction_codes',
         '--vars', json.dumps({'run_id': run_id}),
         '--profiles-dir', '/app/dbt_project',
         '--project-dir', '/app/dbt_project'],
        capture_output=True, text=True
    )
    completed_at = datetime.now(timezone.utc)
    if result.returncode == 0:
        write_run_log_row(run_id, 'silver_transaction_codes', 'SILVER',
                          started_at=started_at, completed_at=completed_at,
                          status='SUCCESS')
        return 'SUCCESS'
    else:
        write_run_log_row(run_id, 'silver_transaction_codes', 'SILVER',
                          started_at=started_at, completed_at=completed_at,
                          status='FAILED', error_message=sanitise_error(result.stderr))
        raise RuntimeError('silver_transaction_codes dbt run failed')


SILVER_ACCOUNTS_PATH = 'data/silver/accounts/data.parquet'


def _write_accounts_quarantine(target_date: str, run_id: str) -> None:
    quarantine_dir = f'data/silver/quarantine/date={target_date}'
    quarantine_path = f'{quarantine_dir}/rejected_accounts.parquet'
    os.makedirs(quarantine_dir, exist_ok=True)
    conn = duckdb.connect()
    conn.execute(f"""
        COPY (
            WITH bronze AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY account_id ORDER BY _source_row_number DESC
                       ) AS rn
                FROM read_parquet('data/bronze/accounts/date={target_date}/data.parquet')
            ),
            deduplicated AS (SELECT * EXCLUDE (rn) FROM bronze WHERE rn = 1),
            null_check AS (
                SELECT *,
                    CASE
                        WHEN account_id IS NULL OR TRIM(CAST(account_id AS VARCHAR)) = ''
                           OR open_date IS NULL OR credit_limit IS NULL
                           OR current_balance IS NULL OR billing_cycle_start IS NULL
                           OR billing_cycle_end IS NULL OR account_status IS NULL
                           OR TRIM(CAST(account_status AS VARCHAR)) = ''
                        THEN 'NULL_REQUIRED_FIELD'
                    END AS _rejection_reason
                FROM deduplicated
            ),
            status_check AS (
                SELECT *,
                    CASE
                        WHEN _rejection_reason IS NULL
                             AND account_status NOT IN ('ACTIVE', 'SUSPENDED', 'CLOSED')
                        THEN 'INVALID_ACCOUNT_STATUS'
                        ELSE _rejection_reason
                    END AS _rejection_reason_final
                FROM null_check
            ),
            rejected AS (
                SELECT * FROM status_check WHERE _rejection_reason_final IS NOT NULL
            )
            SELECT
                account_id, open_date, credit_limit, current_balance,
                billing_cycle_start, billing_cycle_end, account_status,
                _source_file,
                '{run_id}' AS _pipeline_run_id,
                CURRENT_TIMESTAMP AS _rejected_at,
                _rejection_reason_final AS _rejection_reason
            FROM rejected
        ) TO '{quarantine_path}' (FORMAT PARQUET)
    """)
    conn.close()


def run_silver_accounts(target_date: str, run_id: str) -> str:
    if os.path.exists(SILVER_ACCOUNTS_PATH):
        count = duckdb.query(
            f"SELECT COUNT(*) FROM read_parquet('{SILVER_ACCOUNTS_PATH}') "
            f"WHERE _source_file LIKE '%accounts_{target_date}%'"
        ).fetchone()[0]
        if count > 0:
            write_run_log_row(run_id, 'silver_accounts', 'SILVER', status='SKIPPED')
            return 'SKIPPED'
    started_at = datetime.now(timezone.utc)
    silver_exists = os.path.exists(SILVER_ACCOUNTS_PATH)
    result = subprocess.run(
        ['dbt', 'run', '--select', 'silver_accounts',
         '--vars', json.dumps({
             'run_id': run_id,
             'target_date': target_date,
             'silver_exists': 'true' if silver_exists else 'false',
         }),
         '--profiles-dir', '/app/dbt_project',
         '--project-dir', '/app/dbt_project'],
        capture_output=True, text=True
    )
    completed_at = datetime.now(timezone.utc)
    if result.returncode == 0:
        _write_accounts_quarantine(target_date, run_id)
        write_run_log_row(run_id, 'silver_accounts', 'SILVER',
                          started_at=started_at, completed_at=completed_at,
                          status='SUCCESS')
        return 'SUCCESS'
    else:
        write_run_log_row(run_id, 'silver_accounts', 'SILVER',
                          started_at=started_at, completed_at=completed_at,
                          status='FAILED', error_message=sanitise_error(result.stderr))
        raise RuntimeError('silver_accounts dbt run failed')


def _write_transactions_quarantine(target_date: str, run_id: str) -> int:
    quarantine_dir = f'data/silver/quarantine/date={target_date}'
    quarantine_path = f'{quarantine_dir}/rejected.parquet'
    os.makedirs(quarantine_dir, exist_ok=True)

    # After dbt writes the current date's partition, check if OTHER partitions exist
    # for cross-partition dedup (exclude current date to avoid self-match)
    all_silver = glob_mod.glob('data/silver/transactions/**/*.parquet', recursive=True)
    other_silver_exist = any(f'date={target_date}' not in f for f in all_silver)

    if other_silver_exist:
        dedup_cte = f"""
        dedup_joined AS (
            SELECT c.*, sub.transaction_id AS _existing_id
            FROM channel_pass c
            LEFT JOIN (
                SELECT transaction_id
                FROM read_parquet('data/silver/transactions/**/*.parquet',
                                  filename=true, union_by_name=true)
                WHERE filename NOT LIKE '%date={target_date}%'
            ) sub ON c.transaction_id = sub.transaction_id
        ),
        dedup_fail AS (
            SELECT transaction_id, account_id, transaction_date, amount,
                   transaction_code, merchant_name, channel, _source_file, _ingested_at,
                   'DUPLICATE_TRANSACTION_ID' AS _rejection_reason
            FROM dedup_joined WHERE _existing_id IS NOT NULL
        ),"""
    else:
        dedup_cte = """
        dedup_fail AS (
            SELECT transaction_id, account_id, transaction_date, amount,
                   transaction_code, merchant_name, channel, _source_file, _ingested_at,
                   'DUPLICATE_TRANSACTION_ID' AS _rejection_reason
            FROM channel_pass WHERE false
        ),"""

    conn = duckdb.connect()
    conn.execute(f"""
        COPY (
            WITH
            bronze AS (
                SELECT transaction_id, account_id, transaction_date, amount,
                       transaction_code, merchant_name, channel,
                       _source_file, _ingested_at
                FROM read_parquet('data/bronze/transactions/date={target_date}/data.parquet')
            ),
            null_fail AS (
                SELECT *, 'NULL_REQUIRED_FIELD' AS _rejection_reason FROM bronze
                WHERE transaction_id IS NULL OR TRIM(CAST(transaction_id AS VARCHAR)) = ''
                   OR account_id IS NULL OR TRIM(CAST(account_id AS VARCHAR)) = ''
                   OR transaction_date IS NULL OR TRIM(CAST(transaction_date AS VARCHAR)) = ''
                   OR amount IS NULL OR TRIM(CAST(amount AS VARCHAR)) = ''
                   OR transaction_code IS NULL OR TRIM(CAST(transaction_code AS VARCHAR)) = ''
                   OR channel IS NULL OR TRIM(CAST(channel AS VARCHAR)) = ''
            ),
            null_pass AS (
                SELECT * FROM bronze
                WHERE NOT (
                    transaction_id IS NULL OR TRIM(CAST(transaction_id AS VARCHAR)) = ''
                    OR account_id IS NULL OR TRIM(CAST(account_id AS VARCHAR)) = ''
                    OR transaction_date IS NULL OR TRIM(CAST(transaction_date AS VARCHAR)) = ''
                    OR amount IS NULL OR TRIM(CAST(amount AS VARCHAR)) = ''
                    OR transaction_code IS NULL OR TRIM(CAST(transaction_code AS VARCHAR)) = ''
                    OR channel IS NULL OR TRIM(CAST(channel AS VARCHAR)) = ''
                )
            ),
            amount_fail AS (
                SELECT *, 'INVALID_AMOUNT' AS _rejection_reason FROM null_pass
                WHERE TRY_CAST(amount AS DECIMAL(18,4)) IS NULL
                   OR CAST(amount AS DECIMAL(18,4)) <= 0
            ),
            amount_pass AS (
                SELECT * FROM null_pass
                WHERE TRY_CAST(amount AS DECIMAL(18,4)) IS NOT NULL
                  AND CAST(amount AS DECIMAL(18,4)) > 0
            ),
            code_joined AS (
                SELECT b.*, tc.transaction_code AS _tc_match
                FROM amount_pass b
                LEFT JOIN read_parquet('data/silver/transaction_codes/data.parquet') tc
                    ON b.transaction_code = tc.transaction_code
            ),
            code_fail AS (
                SELECT transaction_id, account_id, transaction_date, amount,
                       transaction_code, merchant_name, channel, _source_file, _ingested_at,
                       'INVALID_TRANSACTION_CODE' AS _rejection_reason
                FROM code_joined WHERE _tc_match IS NULL
            ),
            code_pass AS (
                SELECT * EXCLUDE (_tc_match) FROM code_joined WHERE _tc_match IS NOT NULL
            ),
            channel_fail AS (
                SELECT *, 'INVALID_CHANNEL' AS _rejection_reason
                FROM code_pass WHERE channel NOT IN ('ONLINE', 'IN_STORE')
            ),
            channel_pass AS (
                SELECT * FROM code_pass WHERE channel IN ('ONLINE', 'IN_STORE')
            ),
            {dedup_cte}
            all_rejected AS (
                SELECT * FROM null_fail
                UNION ALL SELECT * FROM amount_fail
                UNION ALL SELECT * FROM code_fail
                UNION ALL SELECT * FROM channel_fail
                UNION ALL SELECT * FROM dedup_fail
            )
            SELECT
                transaction_id, account_id, transaction_date, amount,
                transaction_code, merchant_name, channel,
                _source_file,
                '{run_id}' AS _pipeline_run_id,
                CURRENT_TIMESTAMP AS _rejected_at,
                _rejection_reason
            FROM all_rejected
        ) TO '{quarantine_path}' (FORMAT PARQUET)
    """)
    count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{quarantine_path}')").fetchone()[0]
    conn.close()
    return count


def run_silver_transactions(target_date: str, run_id: str) -> str:
    silver_txn_path = f'data/silver/transactions/date={target_date}/data.parquet'
    if os.path.exists(silver_txn_path):
        write_run_log_row(run_id, 'silver_transactions', 'SILVER',
                          status='SKIPPED', records_rejected=None)
        return 'SKIPPED'
    silver_txn_exists = len(
        glob_mod.glob('data/silver/transactions/**/*.parquet', recursive=True)
    ) > 0
    silver_txn_dir = f'data/silver/transactions/date={target_date}'
    os.makedirs(silver_txn_dir, exist_ok=True)
    started_at = datetime.now(timezone.utc)
    result = subprocess.run(
        ['dbt', 'run', '--select', 'silver_transactions',
         '--vars', json.dumps({
             'run_id': run_id,
             'target_date': target_date,
             'silver_txn_exists': 'true' if silver_txn_exists else 'false',
         }),
         '--profiles-dir', '/app/dbt_project',
         '--project-dir', '/app/dbt_project'],
        capture_output=True, text=True
    )
    completed_at = datetime.now(timezone.utc)
    if result.returncode == 0:
        records_rejected = _write_transactions_quarantine(target_date, run_id)
        write_run_log_row(run_id, 'silver_transactions', 'SILVER',
                          started_at=started_at, completed_at=completed_at,
                          status='SUCCESS', records_rejected=records_rejected)
        return 'SUCCESS'
    else:
        write_run_log_row(run_id, 'silver_transactions', 'SILVER',
                          started_at=started_at, completed_at=completed_at,
                          status='FAILED', error_message=sanitise_error(result.stderr))
        raise RuntimeError('silver_transactions dbt run failed')

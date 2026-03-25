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

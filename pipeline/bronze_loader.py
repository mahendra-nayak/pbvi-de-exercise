import os
from datetime import datetime, timezone

from pipeline.bronze_utils import (
    add_audit_columns,
    assert_row_count,
    read_csv_source,
    write_parquet_atomic,
)


def load_bronze_transactions(target_date: str, run_id: str) -> int:
    csv_path = f'source/transactions_{target_date}.csv'
    target_dir = f'data/bronze/transactions/date={target_date}'
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Source file not found: {csv_path}")
    ingested_at = datetime.now(timezone.utc)
    df = read_csv_source(csv_path)
    df = add_audit_columns(df, csv_path, run_id, ingested_at)
    row_count = write_parquet_atomic(df, target_dir)
    canonical_path = os.path.join(target_dir, 'data.parquet')
    assert_row_count(canonical_path, row_count)
    return row_count


def load_bronze_accounts(target_date: str, run_id: str) -> int:
    csv_path = f'source/accounts_{target_date}.csv'
    target_dir = f'data/bronze/accounts/date={target_date}'
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Source file not found: {csv_path}")
    ingested_at = datetime.now(timezone.utc)
    df = read_csv_source(csv_path)
    df = add_audit_columns(df, csv_path, run_id, ingested_at)
    row_count = write_parquet_atomic(df, target_dir)
    canonical_path = os.path.join(target_dir, 'data.parquet')
    assert_row_count(canonical_path, row_count)
    return row_count

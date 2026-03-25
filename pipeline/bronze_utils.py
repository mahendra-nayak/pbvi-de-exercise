import os
from datetime import datetime

import duckdb
import pandas


def read_csv_source(csv_path: str) -> pandas.DataFrame:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Source file not found: {csv_path}")
    with open(csv_path, 'r'):
        pass
    df = pandas.read_csv(csv_path, dtype=str, keep_default_na=False)
    df = df.reset_index()
    df = df.rename(columns={'index': '_source_row_number'})
    df['_source_row_number'] = df['_source_row_number'] + 1
    return df


def add_audit_columns(
    df: pandas.DataFrame,
    source_file: str,
    run_id: str,
    ingested_at: datetime,
) -> pandas.DataFrame:
    for col in ('_source_file', '_ingested_at', '_pipeline_run_id'):
        if col in df.columns:
            raise ValueError(f"Column already exists in DataFrame: {col}")
    df['_source_file'] = os.path.basename(source_file)
    df['_ingested_at'] = ingested_at
    df['_pipeline_run_id'] = run_id
    return df


def write_parquet_atomic(
    df: pandas.DataFrame,
    target_dir: str,
    filename: str = 'data.parquet',
) -> int:
    canonical_path = os.path.join(target_dir, filename)
    temp_path = os.path.join(target_dir, f'.tmp_{filename}')
    if os.path.exists(canonical_path):
        raise RuntimeError(f"Target path already exists: {canonical_path}")
    os.makedirs(target_dir, exist_ok=True)
    df.to_parquet(temp_path, index=False)
    os.rename(temp_path, canonical_path)
    return len(df)


def assert_row_count(parquet_path: str, expected: int) -> None:
    count = duckdb.query(
        f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')"
    ).fetchone()[0]
    if count != expected:
        raise AssertionError(
            f"row count mismatch: parquet={count} expected={expected}"
        )

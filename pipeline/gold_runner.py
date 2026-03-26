import json
import os
import subprocess
from datetime import datetime, timezone

import duckdb

from pipeline.run_log import sanitise_error, write_run_log_row

GOLD_MODELS = {
    'gold_daily_summary': (
        'data/gold/.tmp_daily_summary.parquet',
        'data/gold/daily_summary/data.parquet',
    ),
    'gold_weekly_account_summary': (
        'data/gold/.tmp_weekly_account_summary.parquet',
        'data/gold/weekly_account_summary/data.parquet',
    ),
}


def run_gold(run_id: str) -> None:
    for model_name, (staging_path, canonical_path) in GOLD_MODELS.items():
        if os.path.exists(canonical_path):
            write_run_log_row(run_id, model_name, 'GOLD', status='SKIPPED')
            continue

        canonical_dir = os.path.dirname(canonical_path)
        os.makedirs(canonical_dir, exist_ok=True)

        started_at = datetime.now(timezone.utc)
        result = subprocess.run(
            ['dbt', 'run', '--select', model_name,
             '--vars', json.dumps({'run_id': run_id}),
             '--profiles-dir', '/app/dbt_project',
             '--project-dir', '/app/dbt_project'],
            capture_output=True, text=True
        )
        completed_at = datetime.now(timezone.utc)

        if result.returncode == 0 and os.path.exists(staging_path):
            os.rename(staging_path, canonical_path)
            # INV-08: assert exactly one .parquet file in Gold directory after rename
            parquet_files = [f for f in os.listdir(canonical_dir) if f.endswith('.parquet')]
            assert len(parquet_files) == 1, (
                f'Expected exactly 1 .parquet in {canonical_dir}, found {len(parquet_files)}'
            )
            records_written = duckdb.query(
                f"SELECT COUNT(*) FROM read_parquet('{canonical_path}')"
            ).fetchone()[0]
            write_run_log_row(run_id, model_name, 'GOLD',
                              started_at=started_at, completed_at=completed_at,
                              status='SUCCESS', records_written=records_written)
        else:
            if os.path.exists(staging_path):
                os.remove(staging_path)
            write_run_log_row(run_id, model_name, 'GOLD',
                              started_at=started_at, completed_at=completed_at,
                              status='FAILED', error_message=sanitise_error(result.stderr))
            raise RuntimeError(f'{model_name} dbt run failed')

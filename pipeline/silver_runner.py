import json
import os
import subprocess
from datetime import datetime, timezone

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

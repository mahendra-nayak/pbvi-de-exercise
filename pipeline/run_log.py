import os
import re
from datetime import datetime

import pandas as pd

RUN_LOG_PATH = 'data/pipeline/run_log.parquet'
TEMP_LOG_PATH = 'data/pipeline/.tmp_run_log.parquet'

VALID_PIPELINE_TYPES = frozenset({'HISTORICAL', 'INCREMENTAL'})
VALID_LAYERS         = frozenset({'BRONZE', 'SILVER', 'GOLD'})
VALID_STATUSES       = frozenset({'SUCCESS', 'FAILED', 'SKIPPED'})


def sanitise_error(raw: str, max_chars: int = 500) -> str:
    cleaned = re.sub(r'/\S*', '', raw).strip()
    if len(cleaned) > max_chars:
        cleaned = cleaned[:max_chars - len('[truncated]')] + '[truncated]'
    return cleaned or 'unknown error'


def write_run_log_row(
    run_id: str,
    model_name: str,
    layer: str,
    *,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    pipeline_type: str = 'HISTORICAL',
    status: str,
    records_processed: int | None = None,
    records_written: int | None = None,
    records_rejected: int | None = None,
    error_message: str | None = None,
) -> None:
    if pipeline_type not in VALID_PIPELINE_TYPES:
        raise ValueError(f'pipeline_type must be one of {VALID_PIPELINE_TYPES}, got {pipeline_type!r}')
    if layer not in VALID_LAYERS:
        raise ValueError(f'layer must be one of {VALID_LAYERS}, got {layer!r}')
    if status not in VALID_STATUSES:
        raise ValueError(f'status must be one of {VALID_STATUSES}, got {status!r}')
    if status == 'FAILED' and error_message is None:
        raise ValueError('error_message is required when status is FAILED')
    if status != 'FAILED' and error_message is not None:
        raise ValueError(f'error_message must be None when status is {status!r}')
    if layer != 'SILVER' and records_rejected is not None:
        raise ValueError('records_rejected is only permitted for SILVER layer rows')

    new_row = pd.DataFrame([{
        'run_id': run_id,
        'pipeline_type': pipeline_type,
        'model_name': model_name,
        'layer': layer,
        'started_at': started_at,
        'completed_at': completed_at,
        'status': status,
        'records_processed': records_processed,
        'records_written': records_written,
        'records_rejected': records_rejected,
        'error_message': error_message,
    }])

    os.makedirs(os.path.dirname(RUN_LOG_PATH), exist_ok=True)
    if os.path.exists(RUN_LOG_PATH):
        existing = pd.read_parquet(RUN_LOG_PATH)
        combined = pd.concat([existing, new_row], ignore_index=True)
    else:
        combined = new_row
    combined.to_parquet(TEMP_LOG_PATH, index=False)
    os.replace(TEMP_LOG_PATH, RUN_LOG_PATH)

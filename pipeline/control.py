import os
from datetime import date, datetime, timezone

import pandas as pd

CONTROL_PATH      = 'data/pipeline/control.parquet'
TEMP_CONTROL_PATH = 'data/pipeline/.tmp_control.parquet'


def read_watermark() -> date | None:
    if not os.path.exists(CONTROL_PATH):
        return None
    df = pd.read_parquet(CONTROL_PATH)
    if len(df) != 1:
        raise ValueError(f'control.parquet must have 1 row, has {len(df)}')
    val = df['last_processed_date'].iloc[0]
    return val if isinstance(val, date) else val.date()


def write_watermark(d: date, run_id: str) -> None:
    os.makedirs(os.path.dirname(CONTROL_PATH), exist_ok=True)
    df = pd.DataFrame([{
        'last_processed_date': d,
        'updated_at': datetime.now(timezone.utc),
        'updated_by_run_id': run_id,
    }])
    df.to_parquet(TEMP_CONTROL_PATH, index=False)
    os.replace(TEMP_CONTROL_PATH, CONTROL_PATH)

SELECT
    transaction_code,
    transaction_type,
    description,
    CAST(affects_balance AS BOOLEAN) AS affects_balance,
    debit_credit_indicator,
    _source_file,
    _ingested_at          AS _bronze_ingested_at,
    _pipeline_run_id,
    CURRENT_TIMESTAMP     AS _promoted_at
FROM read_parquet('data/bronze/transaction_codes/data.parquet')
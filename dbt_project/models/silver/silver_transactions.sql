{{ config(
    materialized='external',
    location='data/silver/transactions/date=' ~ var('target_date') ~ '/data.parquet',
    file_format='parquet'
) }}

{% set silver_txn_exists = var('silver_txn_exists', 'false') == 'true' %}

WITH

bronze AS (
    SELECT
        transaction_id, account_id, transaction_date, amount,
        transaction_code, merchant_name, channel,
        _source_file, _ingested_at
    FROM read_parquet('data/bronze/transactions/date={{ var("target_date") }}/data.parquet')
),

-- Stage 1: NULL check — pass vs fail
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

-- Stage 2: AMOUNT check — pass vs fail
amount_pass AS (
    SELECT * FROM null_pass
    WHERE TRY_CAST(amount AS DECIMAL(18,4)) IS NOT NULL
      AND CAST(amount AS DECIMAL(18,4)) > 0
),

-- Stage 3: TRANSACTION CODE check — LEFT JOIN to Silver TC
code_joined AS (
    SELECT b.*, tc.debit_credit_indicator
    FROM amount_pass b
    LEFT JOIN read_parquet('data/silver/transaction_codes/data.parquet') tc
        ON b.transaction_code = tc.transaction_code
),
code_pass AS (
    SELECT * FROM code_joined WHERE debit_credit_indicator IS NOT NULL
),

-- Stage 4: CHANNEL check
channel_pass AS (
    SELECT * FROM code_pass WHERE channel IN ('ONLINE', 'IN_STORE')
),

-- Stage 5: DUPLICATE check — cross-partition scan (guarded for first run)
{% if silver_txn_exists %}
dedup_joined AS (
    SELECT c.*, existing.transaction_id AS _existing_id
    FROM channel_pass c
    LEFT JOIN read_parquet('data/silver/transactions/**/*.parquet', union_by_name=true) existing
        ON c.transaction_id = existing.transaction_id
),
dedup_pass AS (
    SELECT * EXCLUDE (_existing_id) FROM dedup_joined WHERE _existing_id IS NULL
),
{% else %}
dedup_pass AS (SELECT * FROM channel_pass),
{% endif %}

-- Sign assignment via debit_credit_indicator
cte_signed AS (
    SELECT *,
        CASE
            WHEN debit_credit_indicator = 'DR' THEN CAST(amount AS DECIMAL(18,4))
            WHEN debit_credit_indicator = 'CR' THEN -CAST(amount AS DECIMAL(18,4))
            ELSE NULL
        END AS _signed_amount
    FROM dedup_pass
),

-- Account resolution — unresolvable records enter Silver with flag, not quarantine
cte_resolvable AS (
    SELECT s.*, (acct.account_id IS NOT NULL) AS _is_resolvable
    FROM cte_signed s
    LEFT JOIN read_parquet('data/silver/accounts/data.parquet') acct
        ON s.account_id = acct.account_id
)

SELECT
    transaction_id,
    account_id,
    transaction_date,
    amount,
    transaction_code,
    merchant_name,
    channel,
    _source_file,
    _ingested_at                          AS _bronze_ingested_at,
    '{{ var("run_id") }}'                 AS _pipeline_run_id,
    CURRENT_TIMESTAMP                     AS _promoted_at,
    _is_resolvable,
    _signed_amount
FROM cte_resolvable

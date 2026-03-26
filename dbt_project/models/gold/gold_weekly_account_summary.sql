{{ config(
    materialized='external',
    location='data/gold/.tmp_weekly_account_summary.parquet',
    file_format='parquet'
) }}

WITH

base AS (
    SELECT
        t.account_id,
        DATE_TRUNC('week', CAST(t.transaction_date AS DATE))::DATE                          AS week_start_date,
        (DATE_TRUNC('week', CAST(t.transaction_date AS DATE)) + INTERVAL 6 DAYS)::DATE     AS week_end_date,
        CAST(t._signed_amount AS DECIMAL(18,4))                                             AS _signed_amount,
        tc.transaction_type
    FROM read_parquet('data/silver/transactions/**/*.parquet', union_by_name=true) t
    JOIN read_parquet('data/silver/transaction_codes/data.parquet') tc
        ON t.transaction_code = tc.transaction_code
    WHERE t._is_resolvable = true
),

aggregated AS (
    SELECT
        account_id,
        week_start_date,
        week_end_date,
        COUNT(*) FILTER (WHERE transaction_type = 'PURCHASE')                AS total_purchases,
        AVG(CASE WHEN transaction_type = 'PURCHASE'
                 THEN _signed_amount END)                                    AS avg_purchase_amount,
        SUM(CASE WHEN transaction_type = 'PAYMENT'
                 THEN _signed_amount ELSE CAST(0 AS DECIMAL(18,4)) END)     AS total_payments,
        SUM(CASE WHEN transaction_type = 'FEE'
                 THEN _signed_amount ELSE CAST(0 AS DECIMAL(18,4)) END)     AS total_fees,
        SUM(CASE WHEN transaction_type = 'INTEREST'
                 THEN _signed_amount ELSE CAST(0 AS DECIMAL(18,4)) END)     AS total_interest
    FROM base
    GROUP BY account_id, week_start_date, week_end_date
)

SELECT
    account_id,
    week_start_date,
    week_end_date,
    total_purchases,
    avg_purchase_amount,
    total_payments,
    total_fees,
    total_interest,
    (
        SELECT current_balance
        FROM read_parquet('data/silver/accounts/data.parquet')
        WHERE account_id = aggregated.account_id
          AND _record_valid_from::DATE <= aggregated.week_end_date
        ORDER BY _record_valid_from DESC
        LIMIT 1
    )                          AS closing_balance,
    CURRENT_TIMESTAMP          AS _computed_at,
    '{{ var("run_id") }}'      AS _pipeline_run_id
FROM aggregated

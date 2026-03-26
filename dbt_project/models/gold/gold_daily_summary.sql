{{ config(
    materialized='external',
    location='data/gold/.tmp_daily_summary.parquet',
    file_format='parquet'
) }}

WITH

silver AS (
    SELECT
        t.transaction_date,
        t.channel,
        t._is_resolvable,
        CAST(t._signed_amount AS DECIMAL(18,4)) AS _signed_amount,
        tc.transaction_type
    FROM read_parquet('data/silver/transactions/**/*.parquet', union_by_name=true) t
    JOIN read_parquet('data/silver/transaction_codes/data.parquet') tc
        ON t.transaction_code = tc.transaction_code
),

-- Pass 1: resolvable aggregations
pass1 AS (
    SELECT
        transaction_date,
        COUNT(*)                                     AS total_transactions,
        SUM(_signed_amount)                          AS total_signed_amount,
        COUNT(*) FILTER (WHERE channel = 'ONLINE')   AS online_transactions,
        COUNT(*) FILTER (WHERE channel = 'IN_STORE') AS instore_transactions
    FROM silver
    WHERE _is_resolvable = true
    GROUP BY transaction_date
),

-- Pass 2: unresolvable count per date
pass2 AS (
    SELECT
        transaction_date,
        COUNT(*) AS unresolvable_count
    FROM silver
    WHERE _is_resolvable = false
    GROUP BY transaction_date
),

-- Pass 3: transactions_by_type MAP — one entry per distinct type, no hard-coded names
type_counts AS (
    SELECT
        transaction_date,
        transaction_type,
        COUNT(*) AS type_count
    FROM silver
    WHERE _is_resolvable = true
    GROUP BY transaction_date, transaction_type
),
pass3 AS (
    SELECT
        transaction_date,
        MAP(LIST(transaction_type), LIST(type_count)) AS transactions_by_type
    FROM type_counts
    GROUP BY transaction_date
),

-- Final join — pass1 drives rows; dates with zero resolvable records produce no row
joined AS (
    SELECT
        p1.transaction_date,
        p1.total_transactions,
        p1.total_signed_amount,
        p3.transactions_by_type,
        p1.online_transactions,
        p1.instore_transactions,
        COALESCE(p2.unresolvable_count, 0) AS unresolvable_count
    FROM pass1 p1
    LEFT JOIN pass2 p2 ON p1.transaction_date = p2.transaction_date
    LEFT JOIN pass3 p3 ON p1.transaction_date = p3.transaction_date
)

SELECT
    transaction_date,
    total_transactions,
    total_signed_amount,
    transactions_by_type,
    online_transactions,
    instore_transactions,
    unresolvable_count,
    CURRENT_TIMESTAMP                     AS _computed_at,
    '{{ var("run_id") }}'                 AS _pipeline_run_id,
    MIN(transaction_date) OVER ()         AS _source_period_start,
    MAX(transaction_date) OVER ()         AS _source_period_end
FROM joined

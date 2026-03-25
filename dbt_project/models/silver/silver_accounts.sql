{% set silver_path = 'data/silver/accounts/data.parquet' %}
{% set silver_exists = var('silver_exists', 'false') == 'true' %}

WITH bronze AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY _source_row_number DESC) AS rn
    FROM read_parquet('data/bronze/accounts/date={{ var("target_date") }}/data.parquet')
),
deduplicated AS (
    SELECT * EXCLUDE (rn) FROM bronze WHERE rn = 1
),
null_check AS (
    SELECT *,
        CASE
            WHEN account_id IS NULL OR TRIM(CAST(account_id AS VARCHAR)) = ''
               OR open_date IS NULL OR credit_limit IS NULL
               OR current_balance IS NULL OR billing_cycle_start IS NULL
               OR billing_cycle_end IS NULL OR account_status IS NULL
               OR TRIM(CAST(account_status AS VARCHAR)) = ''
            THEN 'NULL_REQUIRED_FIELD'
        END AS _rejection_reason
    FROM deduplicated
),
status_check AS (
    SELECT *,
        CASE
            WHEN _rejection_reason IS NULL
                 AND account_status NOT IN ('ACTIVE', 'SUSPENDED', 'CLOSED')
            THEN 'INVALID_ACCOUNT_STATUS'
            ELSE _rejection_reason
        END AS _rejection_reason_final
    FROM null_check
),
valid AS (
    SELECT * EXCLUDE (_rejection_reason, _rejection_reason_final)
    FROM status_check
    WHERE _rejection_reason_final IS NULL
),
new_silver AS (
    SELECT
        account_id,
        open_date,
        credit_limit,
        current_balance,
        billing_cycle_start,
        billing_cycle_end,
        account_status,
        _source_file,
        _ingested_at AS _bronze_ingested_at,
        '{{ var("run_id") }}' AS _pipeline_run_id,
        CURRENT_TIMESTAMP AS _record_valid_from
    FROM valid
)
{% if silver_exists %}
, retained AS (
    SELECT * FROM read_parquet('{{ silver_path }}')
    WHERE account_id NOT IN (SELECT account_id FROM new_silver)
)
SELECT * FROM new_silver
UNION ALL
SELECT * FROM retained
{% else %}
SELECT * FROM new_silver
{% endif %}

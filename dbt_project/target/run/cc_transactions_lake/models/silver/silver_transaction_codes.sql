create or replace view "memory"."main"."silver_transaction_codes__dbt_int" as (
        select * from 'data/silver/transaction_codes/data.parquet'
    );
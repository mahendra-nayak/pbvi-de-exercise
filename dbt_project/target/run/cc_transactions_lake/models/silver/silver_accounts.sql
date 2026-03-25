create or replace view "memory"."main"."silver_accounts__dbt_int" as (
        select * from 'data/silver/accounts/data.parquet'
    );
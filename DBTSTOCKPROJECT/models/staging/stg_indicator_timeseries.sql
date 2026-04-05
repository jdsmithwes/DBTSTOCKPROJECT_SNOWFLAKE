{{ config(materialized='table') }}

with source_data as (
    select
        *,
        'INDUSTRYBASED_ECONOMIC_LEADING_SNOWFLAKE'          as data_source,
        current_timestamp           as load_date
    from {{ source('ECONOMIC_INDICATORS', 'INDUSTRY_LEADING_INDICATORS_TIMESERIES') }}
)

select *
from source_data
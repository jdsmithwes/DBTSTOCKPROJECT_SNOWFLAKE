{{ config(materialized='table') }}

with source_data as (
    select
        *,
        'AlphaVantage API'          as data_source,
        current_timestamp           as load_date
    from {{ source('public', 'RAW_STOCK_DATA') }}
)

select *
from source_data
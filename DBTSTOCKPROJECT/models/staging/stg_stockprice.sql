{{ config(materialized='table') }}

with source_data as (
    select
        ticker,
        date,
        open,
        high,
        low,
        close,
        adjusted_close,
        volume,
        dividend_amount,
        split_coefficient
    from {{ source('ALPHAVANTAGE_API', 'RAW_STOCK_DATA') }}
    qualify row_number() over (partition by ticker, date order by adjusted_close desc nulls last) = 1
)

select
    *,
    'AlphaVantage API' as data_source,
    current_timestamp as load_date
from source_data

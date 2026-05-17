{{ config(materialized='table') }}

-- Identifies missing trading days per ticker between each ticker's last loaded
-- date and the most recent completed trading day.
--
-- The backfill Python script (Python_Scripts/backfill_missing_dates.py) reads
-- this table to know exactly which (ticker, date) pairs need to be fetched
-- from the AlphaVantage API and uploaded to S3 for Snowpipe ingestion.
--
-- Grain: one row per (ticker, missing_date).

with actual_prices as (

    select
        ticker,
        date as trading_date
    from {{ ref('stg_stockprice') }}

),

-- Last loaded date per ticker — the gap starts the day after this
last_loaded_per_ticker as (

    select
        ticker,
        max(trading_date) as last_loaded_date
    from actual_prices
    group by ticker

),

-- Overall last loaded date across all tickers — used for logging/summary
overall_last_loaded as (

    select max(last_loaded_date) as overall_last_date
    from last_loaded_per_ticker

),

trading_calendar as (

    select trading_date
    from {{ ref('int_trading_calendar') }}

),

-- Cross join each ticker with every expected trading date after its last load
expected_but_missing as (

    select
        lp.ticker,
        tc.trading_date as missing_date,
        lp.last_loaded_date,
        ol.overall_last_date
    from last_loaded_per_ticker lp
    cross join trading_calendar tc
    cross join overall_last_loaded ol
    where tc.trading_date > lp.last_loaded_date

)

select
    em.ticker,
    em.missing_date,
    em.last_loaded_date,
    em.overall_last_date,
    datediff('day', em.last_loaded_date, current_date()) as days_since_last_load
from expected_but_missing em
-- Exclude dates that are actually present (handles partial loads)
left join actual_prices ap
    on em.ticker = ap.ticker
    and em.missing_date = ap.trading_date
where ap.trading_date is null
order by em.ticker, em.missing_date

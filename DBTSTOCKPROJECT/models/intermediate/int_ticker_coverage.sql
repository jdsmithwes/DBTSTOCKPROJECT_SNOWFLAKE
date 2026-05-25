{{ config(materialized='table') }}

-- Grain: one row per ticker.
-- Classifies each ticker as COMPLETE, DELISTED, or INCOMPLETE based on
-- how recently it traded and what fraction of expected trading days it has data for.
--
-- COMPLETE   — last_date within 14 calendar days of dataset end AND coverage >= 95%
-- DELISTED   — last_date more than 63 calendar days behind dataset end (clearly inactive)
-- INCOMPLETE — everything else (new entrants with short history, data gaps, gray zone)
--
-- Used by mart_ml_features (hard-exclude non-COMPLETE) and mart_delisted_tickers.

with actual_prices as (

    select
        ticker,
        date as trading_date
    from {{ ref('stg_stockprice') }}

),

trading_calendar as (

    select trading_date
    from {{ ref('int_trading_calendar') }}

),

dataset_end as (

    select max(trading_date) as dataset_end_date
    from trading_calendar

),

ticker_bounds as (

    select
        ticker,
        min(trading_date) as first_date,
        max(trading_date) as last_date,
        count(*)          as trading_days_loaded
    from actual_prices
    group by ticker

),

-- Count expected trading days from each ticker's first date to the dataset end date.
-- New entrants are not penalised for missing dates before they existed.
expected_days as (

    select
        tb.ticker,
        count(tc.trading_date) as expected_trading_days
    from ticker_bounds    as tb
    join trading_calendar as tc
        on  tc.trading_date >= tb.first_date
        and tc.trading_date <= (select dataset_end_date from dataset_end)
    group by tb.ticker

),

coverage as (

    select
        tb.ticker,
        tb.first_date,
        tb.last_date,
        tb.trading_days_loaded,
        ed.expected_trading_days,
        round(
            tb.trading_days_loaded::float / nullif(ed.expected_trading_days, 0),
            4
        )                                                           as coverage_pct,
        datediff('day', tb.last_date, de.dataset_end_date)         as calendar_days_since_last_trade,
        de.dataset_end_date
    from ticker_bounds as tb
    join expected_days as ed    on tb.ticker = ed.ticker
    cross join dataset_end as de

)

select
    ticker,
    first_date,
    last_date,
    trading_days_loaded,
    expected_trading_days,
    coverage_pct,
    calendar_days_since_last_trade,
    dataset_end_date,
    case
        when calendar_days_since_last_trade <= 14
             and coverage_pct >= 0.95
            then 'COMPLETE'
        when calendar_days_since_last_trade > 63
            then 'DELISTED'
        else 'INCOMPLETE'
    end as coverage_status
from coverage

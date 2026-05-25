{{ config(
    materialized='table',
    tags=['marts', 'operational']
) }}

-- Grain: one row per delisted ticker.
-- Tickers are DELISTED when their last trading date in stg_stockprice is more than
-- 63 calendar days behind the dataset end (see int_ticker_coverage).
--
-- inferred_reason classifies the type of exit using price behaviour at the last trade date:
--   ACQUISITION_OR_MERGER   — price was >= 85% of historical peak (stable/elevated → likely takeout)
--   BANKRUPTCY_OR_COLLAPSE  — price was < 40% of historical peak (severe deterioration)
--   DISTRESSED_EXIT         — price was 40–85% of peak AND fell > 15% in the 30 days before exit
--   DELISTED_NO_DATA        — no company overview record (AlphaVantage returns empty for confirmed delists)
--   UNKNOWN                 — none of the above patterns matched

with coverage as (

    select *
    from {{ ref('int_ticker_coverage') }}
    where coverage_status = 'DELISTED'

),

prices as (

    select ticker, date, close, adjusted_close, volume
    from {{ ref('stg_stockprice') }}
    where ticker in (select ticker from coverage)

),

company as (

    select ticker, company_name, sector, industry, exchange
    from {{ ref('stg_companyoverview') }}

),

last_day as (

    select
        p.ticker,
        p.close          as last_close,
        p.adjusted_close as last_adjusted_close,
        p.volume         as last_volume
    from prices   as p
    join coverage as c
        on  p.ticker = c.ticker
        and p.date   = c.last_date

),

peak as (

    select
        ticker,
        max(adjusted_close) as peak_adjusted_close
    from prices
    group by ticker

),

-- 31st most-recent row ≈ 30 trading days before last_date; used to measure
-- momentum into the delisting event. NULL when history is shorter than 31 rows.
price_30d_before as (

    select p.ticker, p.adjusted_close as adjusted_close_30d_before
    from prices   as p
    join coverage as c on p.ticker = c.ticker
    where p.date <= c.last_date
    qualify row_number() over (partition by p.ticker order by p.date desc) = 31

)

select
    c.ticker,
    co.company_name,
    co.sector,
    co.industry,
    co.exchange,
    c.first_date,
    c.last_date,
    c.trading_days_loaded                                                      as trading_days,
    c.calendar_days_since_last_trade,
    ld.last_close,
    ld.last_volume,
    pk.peak_adjusted_close,
    round(
        ld.last_adjusted_close / nullif(pk.peak_adjusted_close, 0),
        4
    )                                                                          as price_pct_of_peak_at_exit,
    round(
        ld.last_adjusted_close / nullif(p30.adjusted_close_30d_before, 0) - 1,
        4
    )                                                                          as price_30d_return_at_exit,
    case
        when co.ticker is null
            then 'DELISTED_NO_DATA'
        when ld.last_adjusted_close / nullif(pk.peak_adjusted_close, 0) >= 0.85
            then 'ACQUISITION_OR_MERGER'
        when ld.last_adjusted_close / nullif(pk.peak_adjusted_close, 0) < 0.40
            then 'BANKRUPTCY_OR_COLLAPSE'
        when (ld.last_adjusted_close / nullif(p30.adjusted_close_30d_before, 0) - 1) < -0.15
            then 'DISTRESSED_EXIT'
        else 'UNKNOWN'
    end                                                                        as inferred_reason
from coverage           as c
left join company       as co  on c.ticker = co.ticker
left join last_day      as ld  on c.ticker = ld.ticker
left join peak          as pk  on c.ticker = pk.ticker
left join price_30d_before as p30 on c.ticker = p30.ticker

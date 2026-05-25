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

-- Single scan of stg_stockprice: rank rows newest-first per ticker so that
-- row 1 = last trading day and row 31 ≈ 30 trading days before last day.
-- All aggregates (peak, last-day stats, 30d-before price) are computed here.
price_stats as (

    select
        ticker,
        max(adjusted_close)                                  as peak_adjusted_close,
        max(case when rn = 1  then close          end)       as last_close,
        max(case when rn = 1  then adjusted_close end)       as last_adjusted_close,
        max(case when rn = 1  then volume         end)       as last_volume,
        max(case when rn = 31 then adjusted_close end)       as adjusted_close_30d_before
    from (
        select
            ticker,
            date,
            close,
            adjusted_close,
            volume,
            row_number() over (partition by ticker order by date desc) as rn
        from {{ ref('stg_stockprice') }}
        where ticker in (select ticker from coverage)
    )
    group by ticker

),

company as (

    select ticker, company_name, sector, industry, exchange
    from {{ ref('stg_companyoverview') }}

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
    ps.last_close,
    ps.last_volume,
    ps.peak_adjusted_close,
    round(
        ps.last_adjusted_close / nullif(ps.peak_adjusted_close, 0),
        4
    )                                                                          as price_pct_of_peak_at_exit,
    round(
        ps.last_adjusted_close / nullif(ps.adjusted_close_30d_before, 0) - 1,
        4
    )                                                                          as price_30d_return_at_exit,
    case
        when co.ticker is null
            then 'DELISTED_NO_DATA'
        when ps.last_adjusted_close / nullif(ps.peak_adjusted_close, 0) >= 0.85
            then 'ACQUISITION_OR_MERGER'
        when ps.last_adjusted_close / nullif(ps.peak_adjusted_close, 0) < 0.40
            then 'BANKRUPTCY_OR_COLLAPSE'
        when (ps.last_adjusted_close / nullif(ps.adjusted_close_30d_before, 0) - 1) < -0.15
            then 'DISTRESSED_EXIT'
        else 'UNKNOWN'
    end                                                                        as inferred_reason
from coverage           as c
left join company       as co on c.ticker = co.ticker
left join price_stats   as ps on c.ticker = ps.ticker

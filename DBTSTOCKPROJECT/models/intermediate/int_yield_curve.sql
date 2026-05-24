{{ config(materialized='table') }}

-- Fixed income intermediate model. Asset class boundary: only refs fixed income staging.
-- Never joins to equity staging or intermediate models — cross-asset joins happen at mart layer only.
--
-- Grain: one row per NYSE trading date.
-- Forward-fills all series through weekends, holidays, and publication gaps.

with treasury_pivoted as (

    -- Pivot from long (date, maturity, yield) to wide (date, yield_3m, yield_2y, ...)
    select
        date,
        max(case when maturity = '3month' then yield_pct end) as yield_3m,
        max(case when maturity = '2year'  then yield_pct end) as yield_2y,
        max(case when maturity = '5year'  then yield_pct end) as yield_5y,
        max(case when maturity = '7year'  then yield_pct end) as yield_7y,
        max(case when maturity = '10year' then yield_pct end) as yield_10y,
        max(case when maturity = '30year' then yield_pct end) as yield_30y
    from {{ ref('stg_treasury_yields') }}
    group by date

),

fed_funds as (

    select date, rate_pct as fed_funds_rate
    from {{ ref('stg_fed_funds_rate') }}

),

cpi_monthly as (

    select date, cpi_value
    from {{ ref('stg_cpi') }}

),

-- Anchor to trading calendar; join CPI on year-month (monthly → daily)
with_calendar as (

    select
        tc.trading_date                    as date,
        tp.yield_3m,
        tp.yield_2y,
        tp.yield_5y,
        tp.yield_7y,
        tp.yield_10y,
        tp.yield_30y,
        ff.fed_funds_rate,
        cm.cpi_value
    from {{ ref('int_trading_calendar') }} tc
    left join treasury_pivoted tp
        on tc.trading_date = tp.date
    left join fed_funds ff
        on tc.trading_date = ff.date
    left join cpi_monthly cm
        on date_trunc('month', tc.trading_date) = cm.date

),

-- Forward-fill: carry last known value through weekends / holidays / gaps
forward_filled as (

    select
        date,

        last_value(yield_3m        ignore nulls) over (order by date rows between unbounded preceding and current row) as yield_3m,
        last_value(yield_2y        ignore nulls) over (order by date rows between unbounded preceding and current row) as yield_2y,
        last_value(yield_5y        ignore nulls) over (order by date rows between unbounded preceding and current row) as yield_5y,
        last_value(yield_7y        ignore nulls) over (order by date rows between unbounded preceding and current row) as yield_7y,
        last_value(yield_10y       ignore nulls) over (order by date rows between unbounded preceding and current row) as yield_10y,
        last_value(yield_30y       ignore nulls) over (order by date rows between unbounded preceding and current row) as yield_30y,
        last_value(fed_funds_rate  ignore nulls) over (order by date rows between unbounded preceding and current row) as fed_funds_rate,
        last_value(cpi_value       ignore nulls) over (order by date rows between unbounded preceding and current row) as cpi_value

    from with_calendar

),

-- Derive curve shape signals and rate-of-change metrics
with_signals as (

    select
        date,
        yield_3m,
        yield_2y,
        yield_5y,
        yield_7y,
        yield_10y,
        yield_30y,
        fed_funds_rate,
        cpi_value,

        -- Curve spread signals (in percentage points)
        yield_10y - yield_2y                 as spread_2s10s,    -- classic recession leading indicator
        yield_10y - yield_3m                 as spread_3m10y,    -- Fed's preferred recession signal
        yield_30y - yield_2y                 as spread_2s30s,    -- long-end demand signal
        yield_10y - fed_funds_rate           as term_premium,    -- compensation for duration risk

        -- Inversion flags (negative spread = inverted = recession risk elevated)
        case when (yield_10y - yield_2y) < 0 then true else false end as is_2s10s_inverted,
        case when (yield_10y - yield_3m) < 0 then true else false end as is_3m10y_inverted,

        -- Rate-of-change in basis points (1 bp = 0.01 pct point)
        (yield_10y - lag(yield_10y, 1) over (order by date)) * 100  as yield_10y_1d_chg_bps,
        (yield_10y - lag(yield_10y, 5) over (order by date)) * 100  as yield_10y_5d_chg_bps,
        (yield_2y  - lag(yield_2y,  1) over (order by date)) * 100  as yield_2y_1d_chg_bps,

        -- CPI year-over-year change (approximate: compare to same trading day ~252 days ago)
        case
            when lag(cpi_value, 252) over (order by date) > 0
            then (cpi_value / lag(cpi_value, 252) over (order by date) - 1) * 100
            else null
        end                                                          as cpi_yoy_pct

    from forward_filled

)

select * from with_signals

{{ config(materialized='table') }}

-- Pivots FRED macro series from long → wide, then forward-fills gaps so every
-- NYSE trading day has a value (FRED publishes on business days; weekends and
-- some holidays have no observation).

with pivoted as (

    select
        date,
        max(case when series_id = 'VIXCLS'        then value end) as vix,
        max(case when series_id = 'DCOILWTICO'    then value end) as oil_price_wti,
        max(case when series_id = 'DCOILBRENTEU'  then value end) as oil_price_brent,
        max(case when series_id = 'T10YIE'        then value end) as inflation_breakeven_10y,
        max(case when series_id = 'BAMLH0A0HYM2'  then value end) as hy_credit_spread
    from {{ ref('stg_fred_macro') }}
    group by date

),

-- Anchor to the NYSE trading calendar so every trading day has a row
with_calendar as (

    select
        tc.trading_date                              as date,
        p.vix,
        p.oil_price_wti,
        p.oil_price_brent,
        p.inflation_breakeven_10y,
        p.hy_credit_spread
    from {{ ref('int_trading_calendar') }} tc
    left join pivoted p on tc.trading_date = p.date

),

-- Forward-fill: carry the last known value through weekends / holidays / gaps
forward_filled as (

    select
        date,

        last_value(vix ignore nulls) over (
            order by date rows between unbounded preceding and current row
        ) as vix,

        last_value(oil_price_wti ignore nulls) over (
            order by date rows between unbounded preceding and current row
        ) as oil_price_wti,

        last_value(oil_price_brent ignore nulls) over (
            order by date rows between unbounded preceding and current row
        ) as oil_price_brent,

        last_value(inflation_breakeven_10y ignore nulls) over (
            order by date rows between unbounded preceding and current row
        ) as inflation_breakeven_10y,

        last_value(hy_credit_spread ignore nulls) over (
            order by date rows between unbounded preceding and current row
        ) as hy_credit_spread

    from with_calendar

)

select * from forward_filled

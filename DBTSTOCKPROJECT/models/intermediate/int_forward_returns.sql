{{ config(materialized='table') }}

with prices as (

    select
        ticker,
        date,
        adjusted_close
    from {{ ref('stg_stockprice') }}

),

with_forward as (

    select
        ticker,
        date,
        adjusted_close,

        -- 3-month forward price: 63 trading days ahead
        lead(adjusted_close, 63) over (partition by ticker order by date) as forward_price_63d,

        -- 1-month forward price: 21 trading days ahead
        lead(adjusted_close, 21) over (partition by ticker order by date) as forward_price_21d

    from prices

)

select
    ticker,
    date,
    adjusted_close as close_price,

    -- 3-month forward return — the primary ML prediction target
    case
        when forward_price_63d is not null and adjusted_close > 0
        then (forward_price_63d / adjusted_close) - 1
        else null
    end as forward_return_3m,

    -- 1-month forward return — alternative shorter-horizon target
    case
        when forward_price_21d is not null and adjusted_close > 0
        then (forward_price_21d / adjusted_close) - 1
        else null
    end as forward_return_1m,

    -- True only when 63 trading days of future data exist; false for the most recent ~3 months
    case when forward_price_63d is not null then true else false end as has_3m_target

from with_forward

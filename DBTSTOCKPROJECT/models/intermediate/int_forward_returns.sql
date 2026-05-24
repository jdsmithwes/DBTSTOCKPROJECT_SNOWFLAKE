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

        -- 1-month forward price: 21 trading days ahead
        lead(adjusted_close, 21)  over (partition by ticker order by date) as forward_price_21d,

        -- 3-month forward price: 63 trading days ahead
        lead(adjusted_close, 63)  over (partition by ticker order by date) as forward_price_63d,

        -- 6-month forward price: 126 trading days ahead
        lead(adjusted_close, 126) over (partition by ticker order by date) as forward_price_126d,

        -- 9-month forward price: 189 trading days ahead
        lead(adjusted_close, 189) over (partition by ticker order by date) as forward_price_189d,

        -- 12-month forward price: 252 trading days ahead
        lead(adjusted_close, 252) over (partition by ticker order by date) as forward_price_252d

    from prices

)

select
    ticker,
    date,
    adjusted_close as close_price,

    -- 1-month forward return
    case
        when forward_price_21d is not null and adjusted_close > 0
        then (forward_price_21d / adjusted_close) - 1
        else null
    end as forward_return_1m,

    -- 3-month forward return — primary ML target
    case
        when forward_price_63d is not null and adjusted_close > 0
        then (forward_price_63d / adjusted_close) - 1
        else null
    end as forward_return_3m,

    -- 6-month forward return
    case
        when forward_price_126d is not null and adjusted_close > 0
        then (forward_price_126d / adjusted_close) - 1
        else null
    end as forward_return_6m,

    -- 9-month forward return
    case
        when forward_price_189d is not null and adjusted_close > 0
        then (forward_price_189d / adjusted_close) - 1
        else null
    end as forward_return_9m,

    -- 12-month forward return
    case
        when forward_price_252d is not null and adjusted_close > 0
        then (forward_price_252d / adjusted_close) - 1
        else null
    end as forward_return_12m,

    -- Target availability flags
    case when forward_price_21d  is not null then true else false end as has_1m_target,
    case when forward_price_63d  is not null then true else false end as has_3m_target,
    case when forward_price_126d is not null then true else false end as has_6m_target,
    case when forward_price_189d is not null then true else false end as has_9m_target,
    case when forward_price_252d is not null then true else false end as has_12m_target

from with_forward

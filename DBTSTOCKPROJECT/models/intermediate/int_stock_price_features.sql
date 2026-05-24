{{ config(materialized='table') }}

with base as (

    select
        ticker,
        date,
        open,
        high,
        low,
        close,
        adjusted_close,
        volume
    from {{ ref('stg_stockprice') }}

),

with_daily_return as (

    select
        *,
        lag(adjusted_close) over (partition by ticker order by date) as prev_adjusted_close,
        (
            adjusted_close
            / nullif(lag(adjusted_close) over (partition by ticker order by date), 0)
        ) - 1 as daily_return
    from base

),

with_rolling as (

    select
        ticker,
        date,
        close,
        adjusted_close,
        volume,
        daily_return,

        -- Backward-looking momentum returns
        (adjusted_close / nullif(lag(adjusted_close, 21) over (partition by ticker order by date), 0)) - 1 as return_1m,
        (adjusted_close / nullif(lag(adjusted_close, 63) over (partition by ticker order by date), 0)) - 1 as return_3m,
        (adjusted_close / nullif(lag(adjusted_close, 126) over (partition by ticker order by date), 0))
        - 1 as return_6m,

        -- Moving averages of adjusted close
        avg(adjusted_close)
            over (partition by ticker order by date rows between 19 preceding and current row)
            as ma_20d,
        avg(adjusted_close)
            over (partition by ticker order by date rows between 49 preceding and current row)
            as ma_50d,
        avg(adjusted_close)
            over (partition by ticker order by date rows between 199 preceding and current row)
            as ma_200d,

        -- Stddev of price (used for Bollinger Bands)
        stddev(adjusted_close)
            over (partition by ticker order by date rows between 19 preceding and current row)
            as stddev_20d_price,

        -- Realized volatility: rolling stddev of daily returns
        stddev(daily_return)
            over (partition by ticker order by date rows between 19 preceding and current row)
            as volatility_20d,
        stddev(daily_return)
            over (partition by ticker order by date rows between 59 preceding and current row)
            as volatility_60d,

        -- Average volume for ratio
        avg(volume)
            over (partition by ticker order by date rows between 19 preceding and current row)
            as avg_volume_20d,

        -- RSI-14 components (simplified MA approach; Wilder smoothing requires recursive CTE)
        avg(case when daily_return > 0 then daily_return else 0 end)
            over (partition by ticker order by date rows between 13 preceding and current row) as avg_gain_14,
        avg(case when daily_return < 0 then abs(daily_return) else 0 end)
            over (partition by ticker order by date rows between 13 preceding and current row) as avg_loss_14

    from with_daily_return

)

select
    ticker,
    date,
    close,
    adjusted_close,
    volume,
    daily_return,

    -- Momentum
    return_1m,
    return_3m,
    return_6m,

    -- Moving averages
    ma_20d,
    ma_50d,
    ma_200d,

    -- Bollinger Band %B: position within the 2-sigma band (0.5 = midline, >1 = above upper band)
    case
        when stddev_20d_price > 0
            then (adjusted_close - ma_20d) / (2 * stddev_20d_price)
    end as bollinger_pct_b,

    -- Price relative to moving averages (>1 means price is above the average)
    case when ma_20d > 0 then adjusted_close / ma_20d end as price_to_ma_20d,
    case when ma_50d > 0 then adjusted_close / ma_50d end as price_to_ma_50d,
    case when ma_200d > 0 then adjusted_close / ma_200d end as price_to_ma_200d,

    -- Volatility
    volatility_20d,
    volatility_60d,

    -- Volume ratio vs 20-day average (>1 = above-average activity)
    case when avg_volume_20d > 0 then volume / avg_volume_20d end as volume_ratio_20d,

    -- RSI-14 (range 0–100; below 30 = oversold, above 70 = overbought)
    case
        when avg_loss_14 = 0 and avg_gain_14 = 0 then NULL
        when avg_loss_14 = 0 then 100
        when avg_gain_14 = 0 then 0
        else 100 - (100 / (1 + avg_gain_14 / avg_loss_14))
    end as rsi_14

from with_rolling

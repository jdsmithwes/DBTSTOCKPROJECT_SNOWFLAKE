{{ config(materialized='table') }}

with company_snapshot as (

    -- Take the latest snapshot per ticker when multiple loads exist
    select
        ticker,
        analyst_target_price,
        analyst_rating_strong_buy,
        analyst_rating_buy,
        analyst_rating_hold,
        analyst_rating_sell,
        analyst_rating_strong_sell,
        percent_insiders,
        percent_institutions
    from {{ ref('stg_companyoverview') }}
    qualify row_number() over (partition by ticker order by load_timestamp desc) = 1

),

latest_price as (

    select
        ticker,
        close as latest_close
    from {{ ref('stg_stockprice') }}
    qualify row_number() over (partition by ticker order by date desc) = 1

),

with_totals as (

    select
        cs.*,
        lp.latest_close,
        coalesce(cs.analyst_rating_strong_buy, 0)
        + coalesce(cs.analyst_rating_buy, 0)
        + coalesce(cs.analyst_rating_hold, 0)
        + coalesce(cs.analyst_rating_sell, 0)
        + coalesce(cs.analyst_rating_strong_sell, 0) as total_analyst_ratings

    from company_snapshot as cs
    left join latest_price as lp on cs.ticker = lp.ticker

)

select
    ticker,

    analyst_target_price,
    analyst_rating_strong_buy,
    analyst_rating_buy,
    analyst_rating_hold,
    analyst_rating_sell,
    analyst_rating_strong_sell,
    total_analyst_ratings,

    -- Net bullish score: weighted consensus normalized to [-2, +2]
    -- Positive = net bullish, negative = net bearish, null = no ratings
    case
        when total_analyst_ratings > 0
            then (
                2.0 * coalesce(analyst_rating_strong_buy, 0)
                + 1.0 * coalesce(analyst_rating_buy, 0)
                - 1.0 * coalesce(analyst_rating_sell, 0)
                - 2.0 * coalesce(analyst_rating_strong_sell, 0)
            ) / total_analyst_ratings
    end as net_bullish_score,

    -- How much do analysts expect the price to move from current levels?
    case
        when latest_close > 0 and analyst_target_price > 0
            then (analyst_target_price / latest_close) - 1
    end as analyst_upside,

    -- Ownership concentration signals
    percent_insiders,
    percent_institutions,
    coalesce(percent_insiders, 0) + coalesce(percent_institutions, 0) as total_aligned_ownership_pct

from with_totals

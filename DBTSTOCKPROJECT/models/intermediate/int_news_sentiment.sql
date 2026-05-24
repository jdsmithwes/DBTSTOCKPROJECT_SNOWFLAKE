{{ config(materialized='table') }}

-- ASSET CLASS BOUNDARY: refs only news/geopolitical staging — never equity or fixed income models.
-- Grain: one row per NYSE trading date.
--
-- Joins two signal sources anchored to the trading calendar:
--   GDELT   — geopolitical event tone and conflict intensity (US-relevant, daily)
--   AV News — topic-level market news sentiment, pivoted from (date, topic) to wide format
--
-- All joins are LEFT: not every trading day has GDELT data (e.g. new file not yet published)
-- and AV news history only starts ~2022. Nulls are expected and handled downstream.

with trading_calendar as (

    select trading_date as date
    from {{ ref('int_trading_calendar') }}

),

gdelt as (

    select * from {{ ref('stg_gdelt_events') }}

),

av as (

    select * from {{ ref('stg_av_news_sentiment') }}

),

av_pivoted as (

    select
        date,

        -- Per-topic sentiment scores (-1 = very bearish, +1 = very bullish)
        avg(case when topic = 'financial_markets'     then avg_sentiment_score end) as av_market_sentiment,
        avg(case when topic = 'economy_macro'         then avg_sentiment_score end) as av_macro_sentiment,
        avg(case when topic = 'economy_monetary'      then avg_sentiment_score end) as av_monetary_sentiment,
        avg(case when topic = 'economy_fiscal'        then avg_sentiment_score end) as av_fiscal_sentiment,
        avg(case when topic = 'earnings'              then avg_sentiment_score end) as av_earnings_sentiment,
        avg(case when topic = 'energy_transportation' then avg_sentiment_score end) as av_energy_sentiment,

        -- Article volume signals
        sum(case when topic = 'financial_markets'     then article_count else 0 end) as av_market_article_count,
        sum(article_count)                                                            as av_total_article_count,

        -- Direction distribution (averaged across topics present for this date)
        avg(bullish_pct) as av_avg_bullish_pct,
        avg(bearish_pct) as av_avg_bearish_pct

    from av
    group by date

)

select
    tc.date,

    -- GDELT geopolitical signals (US-relevant event aggregates)
    gd.avg_tone            as gdelt_avg_tone,
    gd.conflict_intensity  as gdelt_conflict_intensity,
    gd.total_articles      as gdelt_article_volume,
    gd.pct_conflict        as gdelt_pct_conflict,

    -- Bilateral tension signals for key geopolitical relationships
    gd.us_china_tone       as gdelt_us_china_tone,
    gd.us_iran_tone        as gdelt_us_iran_tone,
    gd.us_russia_tone      as gdelt_us_russia_tone,

    -- Regional aggregated tones
    gd.us_europe_tone      as gdelt_us_europe_tone,
    gd.us_mideast_tone     as gdelt_us_mideast_tone,
    gd.us_apac_tone        as gdelt_us_apac_tone,

    -- AlphaVantage topic-level news sentiment
    av.av_market_sentiment,
    av.av_macro_sentiment,
    av.av_monetary_sentiment,
    av.av_fiscal_sentiment,
    av.av_earnings_sentiment,
    av.av_energy_sentiment,

    -- Composite volume and direction signals
    av.av_total_article_count as av_news_volume,
    av.av_avg_bullish_pct     as av_bullish_pct,
    av.av_avg_bearish_pct     as av_bearish_pct

from trading_calendar tc
left join gdelt      gd on tc.date = gd.date
left join av_pivoted av on tc.date = av.date

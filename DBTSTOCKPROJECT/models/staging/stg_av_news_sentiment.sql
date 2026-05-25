{{ config(materialized='ephemeral') }}

-- Staged AlphaVantage news sentiment aggregates.
-- Grain: one row per (date, topic). Deduped on latest load_timestamp.

with source as (

    select * from {{ source('AV_NEWS', 'RAW_AV_NEWS_SENTIMENT') }}

),

deduped as (

    select
        date,
        topic,
        avg_sentiment_score,
        article_count,
        bullish_pct,
        bearish_pct

    from source

    qualify row_number() over (
        partition by date, topic
        order by load_timestamp desc
    ) = 1

)

select
    date,
    topic,
    avg_sentiment_score,
    article_count,
    bullish_pct,
    bearish_pct

from deduped
where
    date is not NULL
    and topic is not NULL

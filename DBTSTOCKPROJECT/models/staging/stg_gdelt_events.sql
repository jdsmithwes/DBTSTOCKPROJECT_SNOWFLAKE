{{ config(materialized='ephemeral') }}

-- Staged daily GDELT geopolitical event aggregates.
-- Grain: one row per date. Deduped on latest load_timestamp.

with source as (

    select * from {{ source('GDELT', 'RAW_GDELT_DAILY') }}

),

deduped as (

    select
        date,
        avg_tone,
        conflict_intensity,
        total_articles,
        total_events,
        pct_conflict,
        us_china_tone,
        us_iran_tone,
        us_russia_tone,
        us_europe_tone,
        us_mideast_tone,
        us_apac_tone

    from source

    qualify row_number() over (
        partition by date
        order by load_timestamp desc
    ) = 1

)

select
    date,
    avg_tone,
    conflict_intensity,
    total_articles,
    total_events,
    pct_conflict,
    us_china_tone,
    us_iran_tone,
    us_russia_tone,
    us_europe_tone,
    us_mideast_tone,
    us_apac_tone

from deduped
where date is not NULL

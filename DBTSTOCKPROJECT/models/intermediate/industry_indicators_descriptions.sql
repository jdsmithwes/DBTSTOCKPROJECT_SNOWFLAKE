{{ config(materialized='table') }}

with meta as (
    select
        indicatorid,
        name,
        classification,
        units,
        industrylist
    from {{ ref('stg_indicator_metadata') }}
),

timeseries as (
    select
        indicatorid,
        value,
        date
    from {{ ref('stg_indicator_timeseries') }}
)

select
    meta.name,
    meta.classification,
    meta.units,
    meta.industrylist,
    timeseries.value,
    timeseries.date
from meta
inner join timeseries on meta.indicatorid = timeseries.indicatorid
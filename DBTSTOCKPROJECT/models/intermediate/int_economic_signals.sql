{{ config(materialized='table') }}

-- Resolves the fan-out grain issue in stock_performance_with_indicators.
-- That model produces one row per (ticker, date, indicator), which is ungroupable
-- for ML. This model aggregates to (indicator_industry, date) grain so a single
-- join to the price spine returns one row per stock per date.

with indicator_detail as (

    select
        name as indicator_name,
        classification as indicator_classification,
        units as indicator_units,
        industrylist,
        value as indicator_value,
        date
    from {{ ref('industry_indicators_descriptions') }}

),

-- Explode comma-separated industry list into one row per (indicator, industry, date)
indicators_by_industry as (

    select
        indicator_detail.indicator_name,
        indicator_detail.indicator_classification,
        indicator_detail.indicator_units,
        trim(f.value::string) as indicator_industry,
        indicator_detail.indicator_value,
        indicator_detail.date
    from indicator_detail,
        lateral flatten(input => split(indicator_detail.industrylist, ',')) as f

)

select
    indicator_industry,
    date,

    count(distinct indicator_name) as indicator_count,

    -- Composite economic activity: simple average across all applicable indicators
    avg(indicator_value) as economic_activity_index,

    -- Classification-level averages for leading/coincident/lagging signals
    avg(case when indicator_classification = 'Leading' then indicator_value end) as avg_leading_value,
    avg(case when indicator_classification = 'Coincident' then indicator_value end) as avg_coincident_value,
    avg(case when indicator_classification = 'Lagging' then indicator_value end) as avg_lagging_value

from indicators_by_industry
group by 1, 2

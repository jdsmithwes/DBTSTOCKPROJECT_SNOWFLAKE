{{
    config(
        materialized='table',
        table_format='iceberg',
        on_schema_change='fail',
        storage_serialization_policy='COMPATIBLE',
        cluster_by=['horizon', 'model_version'],
    )
}}

-- ── Cross-sectional IC per date ───────────────────────────────────────────────
-- IC (Information Coefficient) = Pearson correlation between predicted and actual
-- returns, computed cross-sectionally on each date then averaged.
-- This is the standard hedge fund metric: IC > 0.05 is useful, > 0.10 is strong.

with evaluable as (

    select *
    from {{ ref('mart_ml_predictions') }}
    where actual_return is not null  -- only rows where the outcome has materialised

),

-- Rank predictions and actuals within each (horizon, model_version, date)
-- cross-section for Spearman rank IC.
ranked as (

    select
        *,
        rank() over (
            partition by horizon, model_version, date
            order by predicted_return
        ) as predicted_rank,
        rank() over (
            partition by horizon, model_version, date
            order by actual_return
        ) as actual_rank

    from evaluable

),

-- Per-date cross-sectional IC (one row per horizon + model_version + date)
daily_ic as (

    select
        horizon,
        model_version,
        date,
        count(*)                                     as n_stocks,
        corr(predicted_return, actual_return)        as ic,
        corr(predicted_rank,   actual_rank)          as rank_ic

    from ranked
    group by horizon, model_version, date
    having count(*) >= 20  -- require at least 20 stocks for a meaningful IC

),

-- Overall aggregate metrics per horizon + model_version
overall as (

    select
        horizon,
        model_version,
        'ALL'                                             as sector,
        count(*)                                          as n_predictions,
        count(distinct date)                              as n_dates,
        count(distinct ticker)                            as n_tickers,
        avg(abs_error)                                    as mae,
        sqrt(avg(squared_error))                          as rmse,
        avg(case when directional_correct then 1.0 else 0.0 end)
                                                          as directional_accuracy,
        corr(predicted_return, actual_return)             as ic_overall,
        avg(d.ic)                                         as mean_daily_ic,
        stddev(d.ic)                                      as stddev_daily_ic,
        avg(d.rank_ic)                                    as mean_daily_rank_ic,
        -- IC Information Ratio (mean IC / std IC) — signal consistency
        case
            when stddev(d.ic) > 0
            then avg(d.ic) / stddev(d.ic)
        end                                               as ic_ir

    from evaluable as e
    inner join daily_ic as d
        using (horizon, model_version, date)
    group by horizon, model_version

),

-- Per-sector breakdown per horizon + model_version
by_sector as (

    select
        horizon,
        model_version,
        sector,
        count(*)                                          as n_predictions,
        count(distinct date)                              as n_dates,
        count(distinct ticker)                            as n_tickers,
        avg(abs_error)                                    as mae,
        sqrt(avg(squared_error))                          as rmse,
        avg(case when directional_correct then 1.0 else 0.0 end)
                                                          as directional_accuracy,
        corr(predicted_return, actual_return)             as ic_overall,
        null::float                                       as mean_daily_ic,
        null::float                                       as stddev_daily_ic,
        null::float                                       as mean_daily_rank_ic,
        null::float                                       as ic_ir

    from evaluable
    group by horizon, model_version, sector

)

select * from overall
union all
select * from by_sector

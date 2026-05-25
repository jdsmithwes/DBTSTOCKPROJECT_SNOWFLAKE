{{ config(
    materialized='table',
    table_format='iceberg',
    storage_serialization_policy='COMPATIBLE',
    cluster_by=['date'],
    unique_key=['date'],
    on_schema_change='fail',
    tags=['marts', 'macro']
) }}

-- Macro economic health mart. Grain: one row per NYSE trading date.
--
-- Combines the Federal Funds Rate and CPI into a single daily snapshot with
-- derived signals that characterize the current policy-inflation regime.
-- Designed for retirement savers: plain-English regime labels alongside the
-- underlying metrics so both analysts and family members can interpret conditions.
--
-- Key use cases:
--   1. Identify whether the Fed is ahead of, behind, or in line with inflation
--   2. Classify the current macro regime (overheating, normalizing, stable, etc.)
--   3. Provide equity risk context: restrictive policy compresses equity multiples
--   4. Track the real Fed Funds Rate — the primary cost-of-capital signal

with yield_curve as (

    select
        date,
        fed_funds_rate,
        cpi_value,
        cpi_yoy_pct,
        fed_regime
    from {{ ref('int_yield_curve') }}

),

-- Month-over-month CPI computed at monthly grain to avoid forward-fill noise
cpi_mom as (

    select
        date,
        (cpi_value / nullif(lag(cpi_value, 1) over (order by date), 0) - 1) * 100 as cpi_mom_pct
    from {{ ref('stg_cpi') }}

),

with_regime as (

    select
        yc.*,
        cm.cpi_mom_pct

    from yield_curve as yc
    left join cpi_mom as cm
        on date_trunc('month', yc.date) = cm.date

)

select
    date,

    -- Asset class tag for cross-asset joins
    'MACRO' as asset_class,

    -- Core series
    fed_funds_rate,
    cpi_value,
    cpi_yoy_pct,
    cpi_mom_pct,

    -- Real Fed Funds Rate: positive = restrictive monetary policy,
    -- negative = accommodative (Fed "behind the curve" on inflation)
    case
        when cpi_yoy_pct is not null
            then fed_funds_rate - cpi_yoy_pct
    end as real_fed_funds_rate,

    -- How far CPI is above or below the Fed's 2% inflation target
    case
        when cpi_yoy_pct is not null
            then cpi_yoy_pct - 2.0
    end as inflation_vs_target,

    -- Fed policy direction based on 90-day rate trend (computed in int_yield_curve)
    fed_regime,

    -- Inflation severity relative to the Fed's 2% mandate
    case
        when cpi_yoy_pct is null     then null
        when cpi_yoy_pct < 1.0       then 'DEFLATIONARY'
        when cpi_yoy_pct < 2.0       then 'LOW'
        when cpi_yoy_pct <= 3.0      then 'TARGET'
        when cpi_yoy_pct <= 5.0      then 'ELEVATED'
        else                              'HIGH'
    end as inflation_regime,

    -- Whether Fed policy is tightening, neutral, or easing relative to inflation
    -- Uses ±0.5 pp real-rate buffer to avoid excessive regime flipping
    case
        when cpi_yoy_pct is null                        then null
        when (fed_funds_rate - cpi_yoy_pct) >  0.5     then 'RESTRICTIVE'
        when (fed_funds_rate - cpi_yoy_pct) < -0.5     then 'ACCOMMODATIVE'
        else                                                  'NEUTRAL'
    end as policy_stance,

    -- True when inflation is above the 2% target AND the real rate is still negative —
    -- the Fed is effectively subsidising borrowing while prices are rising
    case
        when cpi_yoy_pct is null then false
        else coalesce(
            (fed_funds_rate - cpi_yoy_pct) < 0 and cpi_yoy_pct > 2.0,
            false
        )
    end as fed_behind_curve,

    -- Plain-English economic health label for retirement portfolio context.
    -- Combines inflation regime with Fed policy direction.
    case
        when cpi_yoy_pct is null then null
        when cpi_yoy_pct > 5.0
             and fed_regime != 'HIKING'                                                then 'STAGFLATION RISK'
        when cpi_yoy_pct > 3.0
             and fed_regime = 'HIKING'                                                 then 'OVERHEATING — Fed hiking'
        when cpi_yoy_pct < 1.0
             and fed_regime = 'CUTTING'                                                then 'DEFLATION RISK — Fed cutting'
        when cpi_yoy_pct between 2.0 and 3.0
             and (fed_funds_rate - cpi_yoy_pct) > 0.5                                 then 'NORMALIZING — restrictive policy working'
        when cpi_yoy_pct <= 3.0
             and (fed_funds_rate - cpi_yoy_pct) between -0.5 and 2.0                  then 'STABLE'
        else 'TRANSITIONING'
    end as economic_health_label

from with_regime

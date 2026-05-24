{{ config(
    materialized='table',
    table_format='iceberg',
    storage_serialization_policy='COMPATIBLE',
    cluster_by=['date'],
    unique_key=['date'],
    on_schema_change='fail',
    tags=['marts', 'fixed_income']
) }}

-- Fixed income analysis mart. Grain: one row per NYSE trading date.
--
-- asset_class = 'FIXED_INCOME' enables cross-asset comparisons with mart_ml_features
-- (asset_class = 'EQUITY') when building unified retirement portfolio views.
--
-- Key use cases:
--   1. Yield curve shape analysis (inversion as recession leading indicator)
--   2. Fed policy regime identification (hiking / cutting / neutral)
--   3. Equity risk premium: compare yield_10y to expected equity returns in mart_ml_features
--   4. Retirement allocation signal: is fixed income currently attractive vs. equities?

with yield_curve as (

    select * from {{ ref('int_yield_curve') }}

),

-- Classify Fed policy regime by comparing current rate to 90 days ago
with_regime as (

    select
        *,
        lag(fed_funds_rate, 90) over (order by date) as fed_funds_90d_ago

    from yield_curve

),

-- Count consecutive inversion days in trailing 1-year window (regime persistence signal)
with_inversion_duration as (

    select
        *,

        case
            when fed_funds_rate > coalesce(fed_funds_90d_ago, fed_funds_rate) + 0.10 then 'HIKING'
            when fed_funds_rate < coalesce(fed_funds_90d_ago, fed_funds_rate) - 0.10 then 'CUTTING'
            else 'NEUTRAL'
        end as fed_regime,

        sum(case when is_2s10s_inverted then 1 else 0 end) over (
            order by date
            rows between 251 preceding and current row
        ) as inversion_days_trailing_1y

    from with_regime

)

select
    date,

    -- Asset class tag for cross-asset joins and filtering
    'FIXED_INCOME'                                           as asset_class,

    -- Yield curve levels (%)
    yield_3m,
    yield_2y,
    yield_5y,
    yield_7y,
    yield_10y,
    yield_30y,
    fed_funds_rate,

    -- Curve shape signals
    spread_2s10s,
    spread_3m10y,
    spread_2s30s,
    term_premium,

    -- Inversion flags
    is_2s10s_inverted,
    is_3m10y_inverted,

    -- Rate-of-change metrics (basis points)
    yield_10y_1d_chg_bps,
    yield_10y_5d_chg_bps,
    yield_2y_1d_chg_bps,

    -- Inflation
    cpi_value,
    cpi_yoy_pct,

    -- Approximate real yield (nominal 10y minus trailing CPI — proxy only)
    case
        when cpi_yoy_pct is not null
        then yield_10y - (cpi_yoy_pct / 100)
        else null
    end                                                      as approx_real_yield_10y,

    -- Fed policy regime
    fed_regime,
    inversion_days_trailing_1y,

    -- Retirement planning signal: 10y yield >= 4% historically indicates
    -- competitive fixed income returns vs. typical equity risk premiums
    case when yield_10y >= 4.0 then true else false end      as fi_attractive_flag

from with_inversion_duration

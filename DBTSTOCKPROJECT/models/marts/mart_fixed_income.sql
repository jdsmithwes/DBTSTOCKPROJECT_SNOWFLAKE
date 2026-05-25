{{ config(
    materialized='table',
    table_format='iceberg',
    storage_serialization_policy='COMPATIBLE',
    cluster_by=['date', 'security_name'],
    unique_key=['date', 'security_name'],
    on_schema_change='fail',
    tags=['marts', 'fixed_income']
) }}

-- Fixed income analysis mart. Grain: one row per (NYSE trading date, security).
--
-- security_name identifies the specific fixed income instrument in each row —
-- mirrors the maturity parameter from the AlphaVantage TREASURY_YIELD API call for
-- Treasury rows, and the function name for Fed Funds and CPI.
-- instrument_type groups instruments by category for filtering.
-- maturity_term carries the original AlphaVantage maturity label (e.g. '10year') for
-- Treasury rows; NULL for Federal Funds Rate and CPI.
-- rate_value is the instrument's yield / rate / index value for that date.
--
-- Date-level derived signals (spreads, inversions, regime) repeat on every row for a
-- given date — they reflect the full fixed income market context, not per-security values.
--
-- asset_class = 'FIXED_INCOME' enables cross-asset comparisons with mart_ml_features
-- (asset_class = 'EQUITY') when building unified retirement portfolio views.
--
-- Key use cases:
--   1. Per-security time series:  WHERE security_name = 'US Treasury 10Y'
--   2. Instrument-type filter:    WHERE instrument_type = 'TREASURY_YIELD'
--   3. Yield curve shape analysis (inversion as recession leading indicator)
--   4. Fed policy regime identification (hiking / cutting / neutral)
--   5. Equity risk premium: compare rate_value (US Treasury 10Y row) to mart_ml_features
--   6. Retirement allocation signal: is fixed income currently attractive vs. equities?

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

-- Compute date-level derived signals; all carry through to every security row
with_signals as (

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
        ) as inversion_days_trailing_1y,

        case
            when cpi_yoy_pct is not null
                then yield_10y - (cpi_yoy_pct / 100)
        end as approx_real_yield_10y,

        coalesce(yield_10y >= 4.0, FALSE) as fi_attractive_flag

    from with_regime

),

-- Unpivot each fixed income instrument into its own row.
-- security_name, instrument_type, and maturity_term are sourced from the AlphaVantage
-- API parameters (maturity for TREASURY_YIELD; function name for the other two series).
-- Date-level signals repeat on every row so any single-security time series retains
-- full market context without requiring a separate join.
securities as (

    select date, 'US Treasury 3M'     as security_name, 'TREASURY_YIELD'  as instrument_type, '3month'  as maturity_term, yield_3m        as rate_value,
           spread_2s10s, spread_3m10y, spread_2s30s, term_premium, is_2s10s_inverted, is_3m10y_inverted,
           yield_10y_1d_chg_bps, yield_10y_5d_chg_bps, yield_2y_1d_chg_bps,
           cpi_yoy_pct, approx_real_yield_10y, fed_regime, inversion_days_trailing_1y, fi_attractive_flag
    from with_signals

    union all

    select date, 'US Treasury 2Y'     as security_name, 'TREASURY_YIELD'  as instrument_type, '2year'   as maturity_term, yield_2y        as rate_value,
           spread_2s10s, spread_3m10y, spread_2s30s, term_premium, is_2s10s_inverted, is_3m10y_inverted,
           yield_10y_1d_chg_bps, yield_10y_5d_chg_bps, yield_2y_1d_chg_bps,
           cpi_yoy_pct, approx_real_yield_10y, fed_regime, inversion_days_trailing_1y, fi_attractive_flag
    from with_signals

    union all

    select date, 'US Treasury 5Y'     as security_name, 'TREASURY_YIELD'  as instrument_type, '5year'   as maturity_term, yield_5y        as rate_value,
           spread_2s10s, spread_3m10y, spread_2s30s, term_premium, is_2s10s_inverted, is_3m10y_inverted,
           yield_10y_1d_chg_bps, yield_10y_5d_chg_bps, yield_2y_1d_chg_bps,
           cpi_yoy_pct, approx_real_yield_10y, fed_regime, inversion_days_trailing_1y, fi_attractive_flag
    from with_signals

    union all

    select date, 'US Treasury 7Y'     as security_name, 'TREASURY_YIELD'  as instrument_type, '7year'   as maturity_term, yield_7y        as rate_value,
           spread_2s10s, spread_3m10y, spread_2s30s, term_premium, is_2s10s_inverted, is_3m10y_inverted,
           yield_10y_1d_chg_bps, yield_10y_5d_chg_bps, yield_2y_1d_chg_bps,
           cpi_yoy_pct, approx_real_yield_10y, fed_regime, inversion_days_trailing_1y, fi_attractive_flag
    from with_signals

    union all

    select date, 'US Treasury 10Y'    as security_name, 'TREASURY_YIELD'  as instrument_type, '10year'  as maturity_term, yield_10y       as rate_value,
           spread_2s10s, spread_3m10y, spread_2s30s, term_premium, is_2s10s_inverted, is_3m10y_inverted,
           yield_10y_1d_chg_bps, yield_10y_5d_chg_bps, yield_2y_1d_chg_bps,
           cpi_yoy_pct, approx_real_yield_10y, fed_regime, inversion_days_trailing_1y, fi_attractive_flag
    from with_signals

    union all

    select date, 'US Treasury 30Y'    as security_name, 'TREASURY_YIELD'  as instrument_type, '30year'  as maturity_term, yield_30y       as rate_value,
           spread_2s10s, spread_3m10y, spread_2s30s, term_premium, is_2s10s_inverted, is_3m10y_inverted,
           yield_10y_1d_chg_bps, yield_10y_5d_chg_bps, yield_2y_1d_chg_bps,
           cpi_yoy_pct, approx_real_yield_10y, fed_regime, inversion_days_trailing_1y, fi_attractive_flag
    from with_signals

)


select
    date,

    -- Security identification (sourced from AlphaVantage API fields)
    security_name,
    instrument_type,
    maturity_term,
    rate_value,

    -- Asset class tag for cross-asset joins and filtering
    'FIXED_INCOME'              as asset_class,

    -- Curve shape signals (date-level; repeat on every security row for this date)
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
    cpi_yoy_pct,

    -- Approximate real yield (nominal 10y minus trailing CPI — proxy only)
    approx_real_yield_10y,

    -- Fed policy regime
    fed_regime,
    inversion_days_trailing_1y,

    -- Retirement planning signal: 10y yield >= 4% historically indicates
    -- competitive fixed income returns vs. typical equity risk premiums
    fi_attractive_flag

from securities

{{ config(
    materialized='table',
    table_format='iceberg',
    storage_serialization_policy='COMPATIBLE',
    cluster_by=['ticker', 'date'],
    unique_key=['ticker', 'date'],
    on_schema_change='fail',
    tags=['marts', 'ml_input']
) }}

-- ML training and inference table. Grain: one row per (ticker, date).
-- Filter to dataset_split = 'training' for model training.
-- Filter to dataset_split = 'inference' for the most recent ~3 months (no target yet).
--
-- Economic signals join on exact date; economic indicator data is typically monthly,
-- so many trading-day rows will have null economic columns. Forward-fill in your
-- Python preprocessing pipeline before training.

with price_features as (

    select * from {{ ref('int_stock_price_features') }}

),

forward_returns as (

    select
        ticker,
        date,
        forward_return_1m,
        forward_return_3m,
        forward_return_6m,
        forward_return_9m,
        forward_return_12m,
        has_1m_target,
        has_3m_target,
        has_6m_target,
        has_9m_target,
        has_12m_target
    from {{ ref('int_forward_returns') }}

),

company_nonfinancial as (

    select
        ticker,
        company_name,
        sector,
        industry,
        country,
        fiscal_year_end
    from {{ ref('company_overview_nonfinancial') }}

),

company_valuation as (

    select
        ticker,
        market_capitalization,
        shares_outstanding,
        shares_float,
        diluted_eps_ttm,
        pe_ratio,
        trailing_pe,
        forward_pe,
        peg_ratio,
        price_to_book_ratio,
        price_to_sales_ratio_ttm,
        revenue_ttm,
        ebitda,
        ev_to_ebitda,
        ev_to_revenue,
        beta
    from {{ ref('company_overview_valuation') }}

),

analyst_signals as (

    select * from {{ ref('int_analyst_signals') }}

),

industry_mapping as (

    select * from {{ ref('int_industry_mapping') }}

),

economic_signals as (

    select * from {{ ref('int_economic_signals') }}

),

macro_signals as (

    select * from {{ ref('int_macro_signals') }}

),

yield_curve as (

    select * from {{ ref('int_yield_curve') }}

),

news_sentiment as (

    select * from {{ ref('int_news_sentiment') }}

),

-- Only tickers with continuous, current price data enter the ML matrix.
-- Delisted and incomplete tickers are excluded to prevent their exit patterns
-- from contaminating feature distributions for active S&P 500 stocks.
complete_tickers as (

    select ticker
    from {{ ref('int_ticker_coverage') }}
    where coverage_status = 'COMPLETE'

),

spine as (

    select
        pf.ticker,
        pf.date,

        -- Price features
        pf.close,
        pf.adjusted_close,
        pf.volume,
        pf.daily_return,
        pf.return_1m,
        pf.return_3m,
        pf.return_6m,
        pf.ma_20d,
        pf.ma_50d,
        pf.ma_200d,
        pf.bollinger_pct_b,
        pf.price_to_ma_20d,
        pf.price_to_ma_50d,
        pf.price_to_ma_200d,
        pf.volatility_20d,
        pf.volatility_60d,
        pf.volume_ratio_20d,
        pf.rsi_14,

        -- Company identity
        cn.company_name,
        cn.sector,
        cn.industry,
        cn.country,

        -- Valuation fundamentals (snapshot — refreshed when overview is re-loaded)
        cv.market_capitalization,
        cv.shares_outstanding,
        cv.shares_float,
        cv.diluted_eps_ttm,
        cv.pe_ratio,
        cv.trailing_pe,
        cv.forward_pe,
        cv.peg_ratio,
        cv.price_to_book_ratio,
        cv.price_to_sales_ratio_ttm,
        cv.revenue_ttm,
        cv.ebitda,
        cv.ev_to_ebitda,
        cv.ev_to_revenue,
        cv.beta,

        -- Analyst signals
        sig.analyst_target_price,
        sig.net_bullish_score,
        sig.analyst_upside,
        sig.total_analyst_ratings,
        sig.percent_insiders,
        sig.percent_institutions,
        sig.total_aligned_ownership_pct,

        -- Industry → indicator bridge
        im.indicator_industry

    from price_features as pf
    inner join complete_tickers as ct on pf.ticker = ct.ticker
    left join company_nonfinancial as cn on pf.ticker = cn.ticker
    left join company_valuation as cv on pf.ticker = cv.ticker
    left join analyst_signals as sig on pf.ticker = sig.ticker
    left join industry_mapping as im on upper(trim(cn.industry)) = im.company_industry

)

select
    sp.ticker,
    sp.date,

    -- Identity
    sp.company_name,
    sp.sector,
    sp.industry,
    sp.country,

    -- Price features
    sp.close,
    sp.adjusted_close,
    sp.volume,
    sp.daily_return,
    sp.return_1m,
    sp.return_3m,
    sp.return_6m,
    sp.ma_20d,
    sp.ma_50d,
    sp.ma_200d,
    sp.bollinger_pct_b,
    sp.price_to_ma_20d,
    sp.price_to_ma_50d,
    sp.price_to_ma_200d,
    sp.volatility_20d,
    sp.volatility_60d,
    sp.volume_ratio_20d,
    sp.rsi_14,

    -- Fundamental features
    sp.market_capitalization,
    sp.shares_outstanding,
    sp.shares_float,
    sp.diluted_eps_ttm,
    sp.pe_ratio,
    sp.trailing_pe,
    sp.forward_pe,
    sp.peg_ratio,
    sp.price_to_book_ratio,
    sp.price_to_sales_ratio_ttm,
    sp.revenue_ttm,
    sp.ebitda,
    sp.ev_to_ebitda,
    sp.ev_to_revenue,
    sp.beta,

    -- Analyst features
    sp.analyst_target_price,
    sp.net_bullish_score,
    sp.analyst_upside,
    sp.total_analyst_ratings,
    sp.percent_insiders,
    sp.percent_institutions,
    sp.total_aligned_ownership_pct,

    -- Economic features (monthly cadence; null on non-reporting dates — forward-fill in Python)
    es.economic_activity_index,
    es.avg_leading_value,
    es.avg_coincident_value,
    es.avg_lagging_value,
    es.indicator_count as econ_indicator_count,

    -- Macro / risk signals from FRED (daily; forward-filled through weekends and gaps)
    ms.vix,
    ms.oil_price_wti,
    ms.oil_price_brent,
    ms.inflation_breakeven_10y,
    ms.hy_credit_spread,

    -- Fixed income signals from yield curve (key inputs for equity risk premium)
    yc.yield_2y,
    yc.yield_10y,
    yc.fed_funds_rate,
    yc.spread_2s10s,
    yc.spread_3m10y,
    yc.is_2s10s_inverted,
    yc.term_premium,
    yc.yield_10y_1d_chg_bps,

    -- Geopolitical / news sentiment signals
    ns.gdelt_avg_tone,
    ns.gdelt_conflict_intensity,
    ns.gdelt_article_volume,
    ns.gdelt_pct_conflict,
    ns.gdelt_us_china_tone,
    ns.gdelt_us_iran_tone,
    ns.gdelt_us_russia_tone,
    ns.gdelt_us_europe_tone,
    ns.gdelt_us_mideast_tone,
    ns.gdelt_us_apac_tone,
    ns.av_market_sentiment,
    ns.av_macro_sentiment,
    ns.av_monetary_sentiment,
    ns.av_fiscal_sentiment,
    ns.av_earnings_sentiment,
    ns.av_energy_sentiment,
    ns.av_news_volume,
    ns.av_bullish_pct,
    ns.av_bearish_pct,

    -- Asset class tag — 'EQUITY' here; 'FIXED_INCOME' in mart_fixed_income
    'EQUITY' as asset_class,

    -- Target variables (all prediction horizons)
    fr.forward_return_1m,
    fr.forward_return_3m,
    fr.forward_return_6m,
    fr.forward_return_9m,
    fr.forward_return_12m,

    -- Target availability flags
    fr.has_1m_target,
    fr.has_3m_target,
    fr.has_6m_target,
    fr.has_9m_target,
    fr.has_12m_target,

    -- dataset_split: 'training' when the shortest horizon (3m) has a complete window;
    -- 'inference' for the most recent ~63 trading days where targets don't exist yet
    case
        when fr.has_3m_target then 'training'
        else 'inference'
    end as dataset_split

from spine as sp
left join forward_returns as fr
    on
        sp.ticker = fr.ticker
        and sp.date = fr.date
left join economic_signals as es
    on
        sp.indicator_industry = es.indicator_industry
        and sp.date = es.date
left join macro_signals as ms
    on sp.date = ms.date
left join yield_curve as yc
    on sp.date = yc.date
left join news_sentiment as ns
    on sp.date = ns.date

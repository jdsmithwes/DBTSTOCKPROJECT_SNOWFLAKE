{{ config(materialized='table') }}

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
        forward_return_3m,
        forward_return_1m,
        has_3m_target
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

    from price_features pf
    left join company_nonfinancial cn on pf.ticker = cn.ticker
    left join company_valuation    cv on pf.ticker = cv.ticker
    left join analyst_signals     sig on pf.ticker = sig.ticker
    left join industry_mapping     im on upper(trim(cn.industry)) = im.company_industry

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
    es.indicator_count          as econ_indicator_count,

    -- Target variable
    fr.forward_return_3m,
    fr.forward_return_1m,
    fr.has_3m_target,

    -- dataset_split: use 'training' rows to train; 'inference' rows for live predictions
    case
        when fr.has_3m_target then 'training'
        else 'inference'
    end as dataset_split

from spine sp
left join forward_returns fr
    on sp.ticker = fr.ticker
    and sp.date  = fr.date
left join economic_signals es
    on sp.indicator_industry    = es.indicator_industry
    and sp.date                 = es.date

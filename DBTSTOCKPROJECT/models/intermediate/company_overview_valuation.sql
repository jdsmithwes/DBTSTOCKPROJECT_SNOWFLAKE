{{ config(materialized='table') }}

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
    book_value,
    price_to_sales_ratio_ttm,
    revenue_ttm,
    revenue_per_share_ttm,
    ebitda,
    ev_to_ebitda,
    ev_to_revenue,
    beta,

    -- Quality factors (Fama-French QMJ style)
    profit_margin,
    return_on_equity_ttm,
    operating_margin_ttm,
    quarterly_earnings_growth_yoy,
    quarterly_revenue_growth_yoy,
    dividend_yield,

    -- 52-week price extremes (momentum / mean-reversion signals)
    week_52_high,
    week_52_low

from {{ref('stg_companyoverview')}}

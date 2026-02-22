{{ config(materialized='table') }}

with src as (

    select
        raw_payload,
        metadata_filename,
        metadata_file_row_number,
        load_timestamp
    from {{ source('public', 'RAW_COMPANY_OVERVIEW') }}

)

select

    -- ============================
    -- Ingestion metadata
    -- ============================

    metadata_filename,
    metadata_file_row_number,
    load_timestamp,

    try_to_timestamp_ntz(raw_payload:"ingest_timestamp"::string) as ingest_timestamp,

    -- ============================
    -- Identifiers
    -- ============================

    raw_payload:"ticker"::string        as ticker,
    raw_payload:"Symbol"::string        as symbol,
    raw_payload:"Name"::string          as company_name,
    raw_payload:"AssetType"::string     as asset_type,
    raw_payload:"Exchange"::string      as exchange,
    raw_payload:"Currency"::string      as currency,
    raw_payload:"Country"::string       as country,
    raw_payload:"Sector"::string        as sector,
    raw_payload:"Industry"::string      as industry,
    raw_payload:"Address"::string       as address,
    raw_payload:"OfficialSite"::string  as official_site,
    raw_payload:"Description"::string   as description,
    raw_payload:"CIK"::string           as cik,
    raw_payload:"FiscalYearEnd"::string as fiscal_year_end,

    -- ============================
    -- Dates
    -- ============================

    try_to_date(raw_payload:"DividendDate"::string)   as dividend_date,
    try_to_date(raw_payload:"ExDividendDate"::string) as ex_dividend_date,
    try_to_date(raw_payload:"LatestQuarter"::string)  as latest_quarter,

    -- ============================
    -- Moving Averages & Price Stats
    -- ============================

    try_to_double(raw_payload:"50DayMovingAverage"::string)  as ma_50d,
    try_to_double(raw_payload:"200DayMovingAverage"::string) as ma_200d,
    try_to_double(raw_payload:"52WeekHigh"::string)          as week_52_high,
    try_to_double(raw_payload:"52WeekLow"::string)           as week_52_low,

    -- ============================
    -- Valuation Ratios
    -- ============================

    try_to_double(raw_payload:"PERatio"::string)           as pe_ratio,
    try_to_double(raw_payload:"TrailingPE"::string)        as trailing_pe,
    try_to_double(raw_payload:"ForwardPE"::string)         as forward_pe,
    try_to_double(raw_payload:"PEGRatio"::string)          as peg_ratio,
    try_to_double(raw_payload:"PriceToBookRatio"::string)  as price_to_book_ratio,
    try_to_double(raw_payload:"PriceToSalesRatioTTM"::string) as price_to_sales_ratio_ttm,
    try_to_double(raw_payload:"EVToEBITDA"::string)        as ev_to_ebitda,
    try_to_double(raw_payload:"EVToRevenue"::string)       as ev_to_revenue,
    try_to_double(raw_payload:"Beta"::string)              as beta,

    -- ============================
    -- Profitability & Growth
    -- ============================

    try_to_double(raw_payload:"ProfitMargin"::string)          as profit_margin,
    try_to_double(raw_payload:"OperatingMarginTTM"::string)    as operating_margin_ttm,
    try_to_double(raw_payload:"ReturnOnAssetsTTM"::string)     as return_on_assets_ttm,
    try_to_double(raw_payload:"ReturnOnEquityTTM"::string)     as return_on_equity_ttm,
    try_to_double(raw_payload:"QuarterlyEarningsGrowthYOY"::string) as quarterly_earnings_growth_yoy,
    try_to_double(raw_payload:"QuarterlyRevenueGrowthYOY"::string)  as quarterly_revenue_growth_yoy,

    -- ============================
    -- Financial Metrics
    -- ============================

    try_to_number(raw_payload:"RevenueTTM"::string)         as revenue_ttm,
    try_to_double(raw_payload:"RevenuePerShareTTM"::string) as revenue_per_share_ttm,
    try_to_number(raw_payload:"GrossProfitTTM"::string)     as gross_profit_ttm,
    try_to_number(raw_payload:"EBITDA"::string)             as ebitda,

    try_to_double(raw_payload:"EPS"::string)            as eps,
    try_to_double(raw_payload:"DilutedEPSTTM"::string)  as diluted_eps_ttm,
    try_to_double(raw_payload:"BookValue"::string)      as book_value,

    -- ============================
    -- Dividends
    -- ============================

    try_to_double(raw_payload:"DividendPerShare"::string) as dividend_per_share,
    try_to_double(raw_payload:"DividendYield"::string)    as dividend_yield,

    -- ============================
    -- Market Structure
    -- ============================

    try_to_number(raw_payload:"MarketCapitalization"::string) as market_capitalization,
    try_to_number(raw_payload:"SharesOutstanding"::string)    as shares_outstanding,
    try_to_number(raw_payload:"SharesFloat"::string)          as shares_float,

    -- ============================
    -- Ownership & Analyst Ratings
    -- ============================

    try_to_double(raw_payload:"PercentInsiders"::string)     as percent_insiders,
    try_to_double(raw_payload:"PercentInstitutions"::string) as percent_institutions,

    try_to_number(raw_payload:"AnalystRatingStrongBuy"::string)  as analyst_rating_strong_buy,
    try_to_number(raw_payload:"AnalystRatingBuy"::string)        as analyst_rating_buy,
    try_to_number(raw_payload:"AnalystRatingHold"::string)       as analyst_rating_hold,
    try_to_number(raw_payload:"AnalystRatingSell"::string)       as analyst_rating_sell,
    try_to_number(raw_payload:"AnalystRatingStrongSell"::string) as analyst_rating_strong_sell,
    try_to_double(raw_payload:"AnalystTargetPrice"::string)      as analyst_target_price

from src
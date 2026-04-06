with stock_prices as (
    select
        ticker,
        date,
        open,
        high,
        low,
        close,
        adjusted_close,
        volume,
        dividend_amount,
        split_coefficient
    from {{ ref('stg_stockprice') }}
),

company as (
    select
        ticker,
        company_name,
        sector,
        industry
    from {{ ref('stg_companyoverview') }}
),

mapping as (
    select
        company_industry,
        indicator_industry
    from {{ ref('int_industry_mapping') }}
),

indicators as (
    select
        name as indicator_name,
        classification as indicator_classification,
        units as indicator_units,
        industrylist as indicator_industrylist,
        value as indicator_value,
        date as indicator_date
    from {{ ref('industry_indicators_descriptions') }}
),

indicators_exploded as (
    select
        indicator_name,
        indicator_classification,
        indicator_units,
        indicator_value,
        indicator_date,
        trim(f.value::string) as indicator_industry
    from indicators,
        lateral flatten(input => split(indicator_industrylist, ',')) f
),

stock_with_company as (
    select
        sp.ticker,
        sp.date,
        sp.open,
        sp.high,
        sp.low,
        sp.close,
        sp.adjusted_close,
        sp.volume,
        sp.dividend_amount,
        sp.split_coefficient,
        co.company_name,
        co.sector,
        co.industry as company_industry,
        m.indicator_industry as mapped_indicator_industry
    from stock_prices sp
    inner join company co on sp.ticker = co.ticker
    left join mapping m on upper(co.industry) = upper(m.company_industry)
)

select
    swc.ticker,
    swc.date,
    swc.open,
    swc.high,
    swc.low,
    swc.close,
    swc.adjusted_close,
    swc.volume,
    swc.dividend_amount,
    swc.split_coefficient,
    swc.company_name,
    swc.sector,
    swc.company_industry,
    swc.mapped_indicator_industry,
    ie.indicator_name,
    ie.indicator_classification,
    ie.indicator_units,
    ie.indicator_value,
    ie.indicator_date
from stock_with_company swc
left join indicators_exploded ie
    on swc.mapped_indicator_industry = ie.indicator_industry
    and swc.date = ie.indicator_date

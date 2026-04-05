{{ config(materialized='table') }}

SELECT
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
book_value
price_to_sales_ratio_ttm,
revenue_ttm,
revenue_per_share_ttm,
ebitda,
ev_to_ebitda,
ev_to_revenue,
beta

from {{ref('stg_companyoverview')}}





    

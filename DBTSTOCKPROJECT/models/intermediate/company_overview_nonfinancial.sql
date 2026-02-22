SELECT
ticker,
company_name,
description,
country,
currency,
industry,
sector,
latest_quarter,
fiscal_year_end

from {{ref('stg_companyoverview')}}

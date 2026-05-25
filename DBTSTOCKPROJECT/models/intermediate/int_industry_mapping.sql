with seed_data as (

    select
        company_industry,
        indicator_industry
    from {{ ref('seed_industry_mapping') }}

)

select
    upper(trim(company_industry)) as company_industry,
    upper(trim(indicator_industry)) as indicator_industry
from seed_data

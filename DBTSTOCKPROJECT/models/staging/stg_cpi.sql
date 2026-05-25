with source as (

    select
        cast(date as date) as date,
        cast(cpi_value as float) as cpi_value,
        cast(load_timestamp as timestamp) as _loaded_at
    from {{ source('ALPHAVANTAGE_FIXED_INCOME', 'RAW_CPI') }}
    where
        cpi_value is not NULL
        and date is not NULL

),

deduplicated as (

    select *
    from source
    qualify row_number() over (
        partition by date
        order by _loaded_at desc
    ) = 1

)

select * from deduplicated

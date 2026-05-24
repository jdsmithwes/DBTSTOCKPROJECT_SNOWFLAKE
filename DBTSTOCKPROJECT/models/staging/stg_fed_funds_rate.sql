with source as (

    select
        cast(date     as date)            as date,
        cast(rate_pct as float)           as rate_pct,
        cast(load_timestamp as timestamp) as _loaded_at
    from {{ source('ALPHAVANTAGE_FIXED_INCOME', 'RAW_FED_FUNDS_RATE') }}
    where rate_pct is not null
      and date     is not null

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

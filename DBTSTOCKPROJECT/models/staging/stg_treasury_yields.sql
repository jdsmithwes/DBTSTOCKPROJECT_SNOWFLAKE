with source as (

    select
        cast(date     as date)         as date,
        cast(maturity as varchar)      as maturity,
        cast(yield_pct as float)       as yield_pct,
        cast(load_timestamp as timestamp) as _loaded_at
    from {{ source('ALPHAVANTAGE_FIXED_INCOME', 'RAW_TREASURY_YIELDS') }}
    where yield_pct   is not null
      and date        is not null

),

deduplicated as (

    select *
    from source
    qualify row_number() over (
        partition by date, maturity
        order by _loaded_at desc
    ) = 1

)

select * from deduplicated

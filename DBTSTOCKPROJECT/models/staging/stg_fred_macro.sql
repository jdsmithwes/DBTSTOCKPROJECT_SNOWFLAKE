with source as (

    select
        cast(series_id as varchar) as series_id,
        cast(series_name as varchar) as series_name,
        cast(date as date) as date,
        cast(value as float) as value,
        cast(load_timestamp as timestamp) as _loaded_at
    from {{ source('FRED_MACRO', 'RAW_FRED_MACRO') }}
    where
        value is not NULL
        and date is not NULL

),

-- Keep most recent load per (series_id, date) in case of re-runs
deduplicated as (

    select *
    from source
    qualify row_number() over (
        partition by series_id, date
        order by _loaded_at desc
    ) = 1

)

select * from deduplicated

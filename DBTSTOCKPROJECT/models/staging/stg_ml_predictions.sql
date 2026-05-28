{{
    config(materialized='ephemeral')
}}

with source as (

    select * from {{ source('ML_PREDICTIONS', 'RAW_ML_PREDICTIONS') }}

),

deduped as (

    select
        ticker,
        date,
        horizon,
        predicted_return,
        dataset_split,
        model_version,
        run_timestamp,
        row_number() over (
            partition by ticker, date, horizon, model_version
            order by run_timestamp desc
        ) as rn

    from source

)

select
    ticker,
    date,
    horizon,
    predicted_return,
    dataset_split,
    model_version,
    to_timestamp_ntz(run_timestamp)::timestamp_ntz(6) as run_timestamp

from deduped
where rn = 1

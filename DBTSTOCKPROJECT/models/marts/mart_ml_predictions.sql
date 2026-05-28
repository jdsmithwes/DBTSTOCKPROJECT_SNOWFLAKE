{{
    config(
        materialized='incremental',
        unique_key=['ticker', 'date', 'horizon', 'model_version'],
        table_format='iceberg',
        on_schema_change='fail',
        storage_serialization_policy='COMPATIBLE',
        cluster_by=['horizon', 'model_version', 'ticker'],
    )
}}

with predictions as (

    select * from {{ ref('stg_ml_predictions') }}

    {% if is_incremental() %}
    -- Only load model versions not yet in this table
    where model_version > (select max(model_version) from {{ this }})
    {% endif %}

),

features as (

    select
        ticker,
        date,
        sector,
        industry,
        forward_return_3m,
        forward_return_6m,
        forward_return_9m,
        forward_return_12m

    from {{ ref('mart_ml_features') }}

),

joined as (

    select
        p.ticker,
        p.date,
        f.sector,
        f.industry,
        p.horizon,
        p.predicted_return,
        p.model_version,
        p.run_timestamp,
        p.dataset_split,

        -- Actual return for this horizon (null for inference rows until data matures)
        case p.horizon
            when '3m'  then f.forward_return_3m
            when '6m'  then f.forward_return_6m
            when '9m'  then f.forward_return_9m
            when '12m' then f.forward_return_12m
        end as actual_return

    from predictions as p
    left join features as f
        on p.ticker = f.ticker
        and p.date  = f.date

),

with_metrics as (

    select
        ticker,
        date,
        sector,
        industry,
        horizon,
        predicted_return,
        actual_return,
        model_version,
        run_timestamp,
        dataset_split,

        -- Row-level evaluation metrics (null when actual_return is not yet available)
        actual_return - predicted_return                      as residual,
        abs(actual_return - predicted_return)                 as abs_error,
        power(actual_return - predicted_return, 2)            as squared_error,

        case
            when actual_return is null then null
            when sign(predicted_return) = sign(actual_return) then true
            else false
        end as directional_correct

    from joined

)

select * from with_metrics

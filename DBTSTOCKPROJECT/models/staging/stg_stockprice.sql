{{ config(materialized='table') }}

  with source_data as (
      select
      *
      from {{ source('public', 'RAW_STOCK_DATA') }}
  )

  select *
  from source_data
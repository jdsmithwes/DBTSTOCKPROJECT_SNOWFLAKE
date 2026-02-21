{{ config(materialized='table') }}

  with source_data as (
      select
      *
      from DBT_STOCKPROJECT.PUBLIC.RAW_STOCK_DATA
  )

  select *
  from source_data
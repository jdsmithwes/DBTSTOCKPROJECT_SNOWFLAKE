{{ config(materialized='table') }}

-- Generates every expected NYSE trading day from the earliest date in the
-- price data through the most recent completed trading day (yesterday or
-- last Friday). Excludes weekends and US market holidays.
--
-- Used by mart_data_gaps to identify which dates are missing per ticker.

with date_spine as (

    select dateadd('day', seq4(), '2020-01-01'::date) as calendar_date
    from table(generator(rowcount => 3000))

),

holidays as (

    select holiday_date
    from {{ ref('us_market_holidays') }}

),

-- Last completed trading day: the most recent weekday that is not a holiday
-- and is strictly before today (today's session may not be closed yet)
last_completed_trading_day as (

    select max(calendar_date) as last_trading_day
    from date_spine
    where
        calendar_date < current_date()
        and dayofweek(calendar_date) not in (0, 6)
        and calendar_date not in (select holidays.holiday_date from holidays)

)

select ds.calendar_date as trading_date
from date_spine as ds
cross join last_completed_trading_day as lctd
where
    ds.calendar_date <= lctd.last_trading_day
    and dayofweek(ds.calendar_date) not in (0, 6)
    and ds.calendar_date not in (select holidays.holiday_date from holidays)

{{ config(materialized='table') }}

-- Generates every expected NYSE trading day from 2020-01-01 through the most
-- recent completed trading day (yesterday or last Friday).
-- Excludes weekends and US market holidays.
--
-- Used by mart_data_gaps to identify which dates are missing per ticker.

with date_spine as (

    select dateadd('day', seq4(), '2020-01-01'::date) as calendar_date
    from table(generator(rowcount => 5000))

),

holidays as (

    select holiday_date
    from {{ ref('us_market_holidays') }}

),

-- Weekdays that are not holidays — the universe of valid trading days.
-- Built once here; reused for both the last-day lookup and the final output.
valid_trading_days as (

    select ds.calendar_date
    from date_spine as ds
    left join holidays as h on ds.calendar_date = h.holiday_date
    where
        dayofweek(ds.calendar_date) not in (0, 6)
        and h.holiday_date is NULL

),

-- Last completed trading day: most recent valid day strictly before today
-- (today's session may not be closed yet).
last_completed_trading_day as (

    select max(calendar_date) as last_trading_day
    from valid_trading_days
    where calendar_date < current_date()

)

select vt.calendar_date as trading_date
from valid_trading_days as vt
cross join last_completed_trading_day as lctd
where vt.calendar_date <= lctd.last_trading_day

-- Cost monitoring: top 20 most expensive queries in the last 7 days.
-- Run in Snowflake worksheet (not as a dbt model) to review warehouse spend.
--
-- Ref: DBT_BEST_PRACTICES_HEDGE_FUND.md §7.3

select
    query_id,
    user_name,
    warehouse_name,
    total_elapsed_time,
    compilation_time,
    credits_used_compute                                        as estimated_credits,
    round(credits_used_compute * 3.00, 4)                      as estimated_cost_usd
from snowflake.account_usage.query_history
where start_time >= current_date - interval '7 days'
order by credits_used_compute desc
limit 20

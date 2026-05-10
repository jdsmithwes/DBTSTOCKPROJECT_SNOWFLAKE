{% test no_weekend_date(model, column_name) %}

-- In Snowflake: DAYOFWEEK returns 0=Sunday, 6=Saturday
select *
from {{ model }}
where dayofweek({{ column_name }}) in (0, 6)

{% endtest %}

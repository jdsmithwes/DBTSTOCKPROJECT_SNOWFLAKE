{% test recency(model, timestamp_column, datepart, interval) %}

select count(*) as row_count
from {{ model }}
where {{ timestamp_column }} >= dateadd({{ datepart }}, -{{ interval }}, current_timestamp())
having count(*) = 0

{% endtest %}

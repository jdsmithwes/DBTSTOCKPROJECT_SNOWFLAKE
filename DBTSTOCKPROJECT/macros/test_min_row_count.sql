{% test min_row_count(model, min_rows=400) %}

select 1
from (
    select count(*) as row_count
    from {{ model }}
) subq
where subq.row_count < {{ min_rows }}

{% endtest %}

{% test not_empty_string(model, column_name) %}

select *
from {{ model }}
where trim({{ column_name }}) = ''

{% endtest %}

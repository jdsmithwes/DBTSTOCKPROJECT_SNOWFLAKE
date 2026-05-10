{% test column_gte_column(model, column_a, column_b) %}

-- Asserts column_a >= column_b, ignoring rows where either is null
select *
from {{ model }}
where
    {{ column_a }} is not null
    and {{ column_b }} is not null
    and {{ column_a }} < {{ column_b }}

{% endtest %}

{% test ohlc_integrity(model, high_col, low_col, open_col, close_col) %}

select *
from {{ model }}
where
    {{ high_col }} < {{ low_col }}
    or {{ high_col }} < {{ open_col }}
    or {{ high_col }} < {{ close_col }}
    or {{ low_col }} > {{ open_col }}
    or {{ low_col }} > {{ close_col }}

{% endtest %}

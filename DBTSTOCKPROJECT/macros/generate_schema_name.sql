{% macro generate_schema_name(custom_schema_name, node) -%}
  {# If +schema is set (staging/intermediate/marts), use it as-is #}
  {%- if custom_schema_name is not none -%}
    {{ custom_schema_name | upper }}
  {%- else -%}
    {{ target.schema | upper }}
  {%- endif -%}
{%- endmacro %}
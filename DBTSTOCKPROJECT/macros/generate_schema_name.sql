{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if node.fqn[1] == 'staging' -%}
        JDS_STG_{{ node.name }}

    {%- elif node.fqn[1] == 'intermediate' -%}
        JDS_INT_{{ node.name }}

    {%- elif node.fqn[1] == 'marts' -%}
        JDS_MART_{{ node.name }}

    {%- else -%}
        {{ node.name }}

    {%- endif -%}

{%- endmacro %}
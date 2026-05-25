{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {%- if target.name in ['prod', 'snowflake'] -%}
            {{ custom_schema_name | trim }}
        {%- else -%}
            DEV_{{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro %}


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
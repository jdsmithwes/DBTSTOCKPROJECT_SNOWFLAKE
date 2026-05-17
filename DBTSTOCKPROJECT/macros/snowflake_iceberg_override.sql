{% macro snowflake__create_table_built_in_sql(relation, compiled_code) -%}
{#-
    Override of dbt-snowflake's built-in Iceberg table macro.
    base_location is only emitted when external_volume is set — Snowflake-managed
    storage (no external volume) does not support the BASE_LOCATION property.
-#}

{%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

{%- set copy_grants = config.get('copy_grants', default=false) -%}
{%- set row_access_policy = config.get('row_access_policy', default=none) -%}
{%- set table_tag = config.get('table_tag', default=none) -%}

{%- set contract_config = config.get('contract') -%}
{%- if contract_config.enforced -%}
    {{- get_assert_columns_equivalent(compiled_code) -}}
    {%- set compiled_code = get_select_subquery(compiled_code) -%}
{%- endif -%}

{%- set sql_header = config.get('sql_header', none) -%}
{{ sql_header if sql_header is not none }}

create or replace iceberg table {{ relation }}
    {%- if contract_config.enforced %}
    {{ get_table_columns_and_constraints() }}
    {%- endif %}
    {{ optional('external_volume', catalog_relation.external_volume, "'") }}
    catalog = 'SNOWFLAKE'
    {%- if catalog_relation.external_volume %}
    base_location = '{{ catalog_relation.base_location }}'
    {%- endif %}
    {{ optional('storage_serialization_policy', catalog_relation.storage_serialization_policy, "'")}}
    {{ optional('max_data_extension_time_in_days', catalog_relation.max_data_extension_time_in_days)}}
    {{ optional('data_retention_time_in_days', catalog_relation.data_retention_time_in_days)}}
    {{ optional('change_tracking', catalog_relation.change_tracking)}}
    {% if row_access_policy -%} with row access policy {{ row_access_policy }} {%- endif %}
    {% if table_tag -%} with tag ({{ table_tag }}) {%- endif %}
    {% if copy_grants -%} copy grants {%- endif %}
as (
    {%- if catalog_relation.cluster_by is not none -%}
    select * from (
        {{ compiled_code }}
    )
    order by (
        {{ catalog_relation.cluster_by }}
    )
    {%- else -%}
    {{ compiled_code }}
    {%- endif %}
    )
;

{% if catalog_relation.cluster_by is not none -%}
alter iceberg table {{ relation }} cluster by ({{ catalog_relation.cluster_by }});
{%- endif -%}

{% if catalog_relation.automatic_clustering and catalog_relation.cluster_by is not none %}
alter iceberg table {{ relation }} resume recluster;
{%- endif -%}

{%- endmacro %}

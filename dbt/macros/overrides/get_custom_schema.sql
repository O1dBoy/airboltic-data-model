{#
"""
This macro generates a custom schema name based on the environment and the provided custom schema name.
It uses the `generate_schema_name_for_env` macro to create the schema name.
"""
#}


{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ generate_schema_name_for_env(custom_schema_name, node) }}
{%- endmacro %}

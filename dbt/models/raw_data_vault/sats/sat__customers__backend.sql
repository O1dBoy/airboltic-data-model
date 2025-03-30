-- Config should include partition by = ['_incremental_date']. Not possible due to limitations
-- on the dbt-spark session method connector.
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
    )
}}

{% set customers_landing = ref('landing__customers') %}

{{ 
    build_sat(
        source_table = customers_landing,
        hub_name = 'customers',
        key_ = 'customer_id',
        effective_datetime_column = 'updated_at',
        columns = [
            'name',
            'email',
            'phone_number',
            'customer_group_id'
        ],
        incremental_column = 'imported_at'
    ) 
}}

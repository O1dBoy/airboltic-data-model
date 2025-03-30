-- Config should include partition by = ['_incremental_date']. Not possible due to limitations
-- on the dbt-spark session method connector.
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
    )
}}

{% set orders_landing = ref('landing__orders') %}

{{ 
    build_hub(
        source_table = orders_landing,
        hub_name = 'orders',
        key_ = 'order_id',
        incremental_column = 'imported_at'
    ) 
}}

-- Config should include partition by = ['_incremental_date']. Not possible due to limitations
-- on the dbt-spark session method connector.
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
    )
}}

{% set orders_landing  = ref('landing__orders') %}

{{ 
    build_link(
        source_table = orders_landing,
        hub_names = ['orders', 'customers', 'trips'],
        keys = ['order_id', 'customer_id', 'trip_id'],
        incremental_column = 'imported_at'
    ) 
}}

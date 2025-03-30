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
    build_sat(
        source_table = orders_landing, 
        hub_name = 'orders',
        key_ = 'order_id',
        effective_datetime_column = 'updated_at',
        columns = [
            'price_eur',
            'seat_no',
            'status',
        ],
        incremental_column = 'imported_at'
    ) 
}}

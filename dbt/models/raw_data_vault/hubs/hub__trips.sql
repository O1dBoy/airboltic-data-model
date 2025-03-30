-- Config should include partition by = ['_incremental_date']. Not possible due to limitations
-- on the dbt-spark session method connector.
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
    )
}}

{% set trips_landing = ref('landing__trips') %}

{{ 
    build_hub(
        source_table = trips_landing,
        hub_name = 'trips',
        key_ = 'trip_id',
        incremental_column = 'imported_at'
    ) 
}}

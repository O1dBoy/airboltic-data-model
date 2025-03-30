-- Config should include partition by = ['_incremental_date']. Not possible due to limitations
-- on the dbt-spark session method connector.
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
    )
}}

{% set trips_landing  = ref('landing__trips') %}

{{ 
    build_link(
        source_table = trips_landing,
        hub_names = ['trips', 'aeroplanes'],
        keys = ['trip_id', 'airplane_id'],
        incremental_column = 'imported_at'
    ) 
}}

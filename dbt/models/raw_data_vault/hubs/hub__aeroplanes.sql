-- Config should include partition by = ['_incremental_date']. Not possible due to limitations
-- on the dbt-spark session method connector.
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
    )
}}

{% set aeroplanes_landing = ref('landing__aeroplanes') %}

{{ 
    build_hub(
        source_table = aeroplanes_landing,
        hub_name = 'aeroplanes',
        key_ = 'airplane_id',
        incremental_column = 'imported_at'
    ) 
}}

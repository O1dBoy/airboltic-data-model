-- Config should include partition by = ['_incremental_date']. Not possible due to limitations
-- on the dbt-spark session method connector.
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
    )
}}

{% set trips_landing = ref('landing__trips') %}

with input as (
    select
        {{ dbt_utils.star(trips_landing, except = ['start_timestamp', 'end_timestamp']) }},
        cast(start_timestamp as timestamp) as start_timestamp,
        cast(end_timestamp as timestamp) as end_timestamp
    from {{ trips_landing }}
),

output as (
    {{
        build_sat(
            source_table = 'input',
            hub_name = 'trips',
            key_ = 'trip_id',
            effective_datetime_column = 'updated_at',
            columns = [
                'origin_city',
                'destination_city',
                'start_timestamp',
                'end_timestamp'
            ],
            incremental_column = 'imported_at'
        ) 
    }}
)

select
    *
from output

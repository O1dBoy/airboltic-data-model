{{ 
    config(
        materialized = 'table'
    ) 
}}

{% set trips_events = ref('trips') %}

select
    `Trip ID` as trip_id,
    `Origin City` as origin_city,
    `Destination City` as destination_city,
    `Airplane ID` as airplane_id,
    `Start Timestamp` as start_timestamp,
    `End Timestamp` as end_timestamp,
    'trips-kafka-topic' as topic,
    cast('20250201' as timestamp) as updated_at,
    20250301 as imported_at
from {{ trips_events }}

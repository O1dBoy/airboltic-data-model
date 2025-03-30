{{ 
    config(
        materialized = 'table'
    ) 
}}

{% set aeroplanes_events = ref('aeroplanes') %}

select
    `Airplane ID` as airplane_id,
    `Airplane Model` as airplane_model,
    `Manufacturer` as manufacturer,
    'aeroplane-events-topic' as topic,
    cast('20250201' as timestamp) as updated_at,
    20250301 as imported_at
from {{ aeroplanes_events }}

{{ 
    config(
        materialized = 'table'
    ) 
}}

{% set customers_groups_events = ref('customer_groups') %}

select
    `ID` as id,
    `Type` as type,
    `Name` as name,
    `Registry number` as registry_number,
    'customers-groups-kafka-topic' as topic,
    cast('20250201' as timestamp) as updated_at,
    20250301 as imported_at
from {{ customers_groups_events }}

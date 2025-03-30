{{ 
    config(
        materialized = 'table'
    ) 
}}

{% set customers_events = ref('customers') %}

select
    `Customer ID` as customer_id,
    `Name` as name,
    `Customer Group ID` as customer_group_id,
    `Email` as email,
    `Phone Number` as phone_number,
    'customers-kafka-topic' as topic,
    cast('20250201' as timestamp) as updated_at,
    20250301 as imported_at
from {{ customers_events }}

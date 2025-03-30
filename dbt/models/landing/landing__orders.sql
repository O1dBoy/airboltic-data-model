{{ 
    config(
        materialized = 'table'
    ) 
}}

{% set orders_events = ref('orders') %}

select
    `Order ID` as order_id,
    `Customer ID` as customer_id,
    `Trip ID` as trip_id,
    `Price (EUR)` as price_eur,
    `Seat No` as seat_no,
    `Status` as status,
    'orders-kafka-topic' as topic,
    cast('20250201' as timestamp) as updated_at,
    20250301 as imported_at
from {{ orders_events }}

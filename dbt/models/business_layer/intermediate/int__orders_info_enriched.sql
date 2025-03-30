-- Config should include:
-- partition by = ['_incremental_date']
-- incremental_strategy = 'merge'
-- file_format = 'delta
-- Not possible due to limitations on the dbt-spark session method connector.
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        unique_key = ['order_id', 'status']
    )
}}

{% set orders_int = ref('int__orders_info') %}
{% set customers_int = ref('int__customers_info') %}
{% set trips_int = ref('int__trips_info') %}
{% set aeroplans_int = ref('int__aeroplanes_info') %}

with orders as (
    select
        *
    from {{ orders_int }}
    where 1 = 1
    and
        {{ 
            incremental_filter(
                execution_start_date = var('execution_start_date'),
                date_column = 'cast(dateadd(day, -1, _incremental_date) as int)'
            ) 
        }}
),

orders_enriched as (
    select
        o.order_id,
        o.price_eur,
        o.seat_no,
        o.status,
        o.customer_id,
        c.customer_group_id,
        c.customer_group_type,
        c.customer_group_name,
        c.customer_group_registry_number,
        o.trip_id,
        t.trip_end_date,
        t.origin_city as trip_origin_city,
        t.destination_city as trip_destination_city,
        t.start_timestamp as trip_start_timestamp,
        t.end_timestamp as trip_end_timestamp,
        t.trip_duration_hours,
        t.aeroplane_id,
        a.aeroplane_model as aeroplane_model,
        a.aeroplane_manufacturer,
        a.max_seats,
        a.max_weight,
        a.max_range,
        a.engine_type,
        o._incremental_date
    from orders as o
    left join {{ customers_int }} c
        on o.customer_id = c.customer_id
    left join {{ trips_int }} t
        on o.trip_id = t.trip_id
    left join {{ aeroplans_int }} a
        on t.aeroplane_id = a.aeroplane_id
)

select * from orders_enriched

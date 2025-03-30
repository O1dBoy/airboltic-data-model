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

{% set hub_orders = ref('hub__orders') %}
{% set sat_orders_backend = ref('sat__orders__backend') %}
{% set link_orders_customers_trips = ref('link__orders__customers__trips') %}
{% set hub_customers = ref('hub__customers') %}
{% set hub_trips = ref('hub__trips') %}

with sat as (
    select
        *
    from {{ sat_orders_backend }}
    where 1 = 1
    and
        {{ 
            incremental_filter(
                execution_start_date = var('execution_start_date'),
                date_column = 'cast(dateadd(day, -1, _incremental_date) as int)'
            ) 
        }}
),

final as (
    select
        ho.hub__orders__nk as order_id,
        hc.hub__customers__nk as customer_id,
        ht.hub__trips__nk as trip_id,
        s.price_eur,
        s.seat_no,
        lower(s.status) as status,
        s._incremental_date
    from sat as s
    inner join {{ hub_orders }} ho
        on s.hub__orders__key = ho.hub__orders__key
    left join {{ link_orders_customers_trips }} l
        on ho.hub__orders__key = l.hub__orders__key
    left join {{ hub_customers }} hc
        on l.hub__customers__key = hc.hub__customers__key
    left join {{ hub_trips }} ht
        on l.hub__trips__key = ht.hub__trips__key
)

select * from final

-- Config should include:
-- partition by = ['consumer_group_id']
-- Not possible due to limitations on the dbt-spark session method connector.
{{
    config(
        materialized = 'table'
    )
}}

{% set orders_enriched = ref('int__orders_info_enriched') %}

with metrics as (
    select
        customer_id,
        customer_group_id,
        customer_group_type,
        customer_group_name,
        count(distinct order_id) as orders_count,
        sum(price_eur) as total_revenue_eur,
        avg(price_eur) as avg_spent_eur,
        count(distinct trip_id) as trips_count,
        sum(trip_duration_hours) as total_trips_duration_hours,
        sum(trip_duration_hours) / count(distinct trip_id) as avg_trip_duration_hours
    from {{ orders_enriched }}
    where 1 = 1
        and status != 'cancelled'
    group by all
)

select * from metrics
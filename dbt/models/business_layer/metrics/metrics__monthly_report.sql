-- Config should include:
-- partition by = ['trip_month']
-- Not possible due to limitations on the dbt-spark session method connector.
{{
    config(
        materialized = 'table'
    )
}}

{% set orders_enriched = ref('int__orders_info_enriched') %}

with metrics as (
    select
        month(trip_end_date) as trip_month,
        count(distinct order_id) as orders_count,
        sum(price_eur) as total_revenue_eur,
        avg(price_eur) as avg_spent_eur,
        count(distinct trip_id) as trips_count,
        sum(trip_duration_hours) as total_trips_duration_hours,
        sum(trip_duration_hours) / count(distinct trip_id) as avg_trip_duration_hours,
        count(distinct customer_id) as active_customers
    from {{ orders_enriched }}
    where 1 = 1
        and status != 'cancelled'
    group by all
)

select * from metrics

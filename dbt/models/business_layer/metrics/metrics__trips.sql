-- Config should include:
-- partition by = ['trip_end_date']
-- Not possible due to limitations on the dbt-spark session method connector.
{{
    config(
        materialized = 'table'
    )
}}

{% set orders_enriched = ref('int__orders_info_enriched') %}

with metrics as (
    select
        trip_id,
        date(trip_end_timestamp) as trip_end_date,
        aeroplane_id,
        aeroplane_manufacturer,
        aeroplane_model,
        max_seats,
        max_range,
        trip_origin_city,
        trip_destination_city,
        count(distinct order_id) as orders_count,
        sum(price_eur) as total_revenue_eur,
        avg(price_eur) as avg_spent_eur,
        count(distinct trip_id) as trips_count,
        sum(trip_duration_hours) as total_trips_duration_hours
    from {{ orders_enriched }}
    where 1 = 1
        and status != 'cancelled'
    group by all
)

select * from metrics

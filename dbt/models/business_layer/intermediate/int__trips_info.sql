-- Config should include:
-- partition by = ['_incremental_date']
-- incremental_strategy = 'merge'
-- file_format = 'delta
-- Not possible due to limitations on the dbt-spark session method connector.
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        unique_key = ['trip_id']
    )
}}

{% set hub_trips = ref('hub__trips') %}
{% set sat_trips_backend = ref('sat__trips__backend') %}
{% set link_trips_aeroplanes = ref('link__trips__aeroplanes') %}
{% set hub_aeroplanes = ref('hub__aeroplanes') %}

with sat as (
    select
        *
    from {{ sat_trips_backend }}
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
        ht.hub__trips__nk as trip_id,
        date(s.end_timestamp) as trip_end_date,
        ha.hub__aeroplanes__nk as aeroplane_id,
        lower(s.origin_city) as origin_city,
        lower(s.destination_city) as destination_city,
        s.start_timestamp,
        s.end_timestamp,
        (unix_timestamp(s.end_timestamp) - unix_timestamp(s.start_timestamp)) / 3600 as trip_duration_hours,
        s._incremental_date
    from sat as s
    inner join {{ hub_trips}} ht
        on s.hub__trips__key = ht.hub__trips__key
    left join {{ link_trips_aeroplanes }} l
        on ht.hub__trips__key = l.hub__trips__key
    left join {{ hub_aeroplanes }} ha
        on l.hub__aeroplanes__key = ha.hub__aeroplanes__key
)

select * from final

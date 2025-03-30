{{ 
    config(
        materialized = 'table',
    )
}}

{% set aeroplane_models = ref('landing__aeroplane_models') %}

with base as (
    select
        key as aeroplane_manufacturer,
        value as models_payload,
        topic
    from {{ aeroplane_models }}
),

exploded as (
    select
        aeroplane_manufacturer,
        explode(models_payload) as (aeroplane_model, details),
        topic
    from base
),

final as (
    select
        lower(aeroplane_model) as aeroplane_model,
        aeroplane_manufacturer,
        details.max_seats as max_seats,
        details.max_distance as max_range,
        details.max_weight as max_weight,
        details.engine_type as engine_type,
        current_timestamp as _load_datetime,
        topic as _source
    from exploded
)

select * from final

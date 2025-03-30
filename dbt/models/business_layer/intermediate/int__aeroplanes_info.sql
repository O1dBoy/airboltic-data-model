-- Config should include:
-- partition by = ['_incremental_date']
-- incremental_strategy = 'merge'
-- file_format = 'delta
-- Not possible due to limitations on the dbt-spark session method connector.
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        unique_key = ['aeroplane_id']
    )
}}

{% set hub_aeroplanes = ref('hub__aeroplanes') %}
{% set sat_aeroplanes_backend = ref('sat__aeroplanes__backend') %}
{% set ref_aeroplane_models = ref('ref__aeroplane_models') %}

with sat as (
    select
        *
    from {{ sat_aeroplanes_backend }}
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
        ha.hub__aeroplanes__nk as aeroplane_id,
        lower(s.aeroplane_manufacturer) as aeroplane_manufacturer,
        s.aeroplane_model,
        r.max_seats,
        r.max_weight,
        r.max_range,
        lower(r.engine_type) as engine_type,
        s._incremental_date
    from sat as s
    inner join {{ hub_aeroplanes }} ha
        on s.hub__aeroplanes__key = ha.hub__aeroplanes__key
    left join {{ ref_aeroplane_models }} r
        on s.aeroplane_model = r.aeroplane_model
)

select * from final

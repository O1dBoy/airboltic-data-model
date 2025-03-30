-- Config should include partition by = ['_incremental_date']. Not possible due to limitations
-- on the dbt-spark session method connector.
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
    )
}}

{% set aeroplanes_landing = ref('landing__aeroplanes') %}

with input as (
    select
        {{ dbt_utils.star(aeroplanes_landing, except = ['airplane_id', 'airplane_model', 'manufacturer']) }},
        airplane_id as aeroplane_id,
        lower(airplane_model) as aeroplane_model,
        manufacturer as aeroplane_manufacturer
    from {{ aeroplanes_landing }}
),

output as (
    {{
        build_sat(
            source_table = 'input',
            hub_name = 'aeroplanes',
            key_ = 'aeroplane_id',
            effective_datetime_column = 'updated_at',
            columns = [
                'aeroplane_model',
                'aeroplane_manufacturer'
            ],
            incremental_column = 'imported_at'
        ) 
    }}
)

select
    *
from output

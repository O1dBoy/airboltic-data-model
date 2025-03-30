-- Config should include:
-- partition by = ['_incremental_date']
-- incremental_strategy = 'merge'
-- file_format = 'delta
-- Not possible due to limitations on the dbt-spark session method connector.
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        unique_key = ['customer_id']
    )
}}

{% set hub_customers = ref('hub__customers') %}
{% set sat_customers_backend = ref('sat__customers__backend') %}
{% set ref_customer_groups = ref('ref__customer_groups') %}


with sat as (
    select
        *
    from {{ sat_customers_backend }}
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
        h.hub__customers__nk as customer_id,
        s.name,
        s.email,
        s.phone_number,
        s.customer_group_id,
        r.customer_group_type,
        r.customer_group_name,
        r.customer_group_registry_number,
        s._incremental_date
    from sat as s
    inner join {{ hub_customers }} h
        on s.hub__customers__key = h.hub__customers__key
    left join {{ ref_customer_groups }} r
        on s.customer_group_id = r.customer_group_id
)

select * from final

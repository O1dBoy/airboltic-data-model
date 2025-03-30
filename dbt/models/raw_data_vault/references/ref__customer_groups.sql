{{ 
    config(
        materialized = 'table',
    )
}}

{% set customer_groups_landing = ref('landing__customer_groups') %}

select
    id as customer_group_id,
    type as customer_group_type,
    name as customer_group_name,
    registry_number as customer_group_registry_number,
    current_timestamp as _load_datetime,
    topic as _source
from {{ customer_groups_landing }}

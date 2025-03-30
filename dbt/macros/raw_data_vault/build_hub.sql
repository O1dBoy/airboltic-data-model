{% macro build_hub(source_table, hub_name, key_, incremental_column) %}

with base as (
    select
        {{ concat_and_hash(include=[key_]) }} as hub__{{ hub_name }}__key,
        {{ key_ }} as hub__{{ hub_name }}__nk,
        topic as _source,
        current_timestamp as _load_datetime,
        date(current_timestamp) as _incremental_date
    from {{ source_table }}
    {% if is_incremental() %}
    where 1 = 1
        and
        {{ 
            incremental_filter(
                execution_start_date = var('execution_start_date'),
                execution_end_date = var('execution_end_date'),
                date_column = incremental_column
            ) 
        }}
    {% endif %}
),

deduped_step1 as (
     select
        *
    from base b
    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} h
        where b.hub__{{ hub_name }}__key = h.hub__{{ hub_name }}__key
    )
    {% endif %}
),

deduped_step2 as (
    select
        *,
        row_number() over (
            partition by hub__{{ hub_name }}__key
            order by _load_datetime desc
        ) as row_num
    from deduped_step1
),

final as (
    select
        hub__{{ hub_name }}__key,
        hub__{{ hub_name }}__nk,
        _source,
        _load_datetime,
        _incremental_date
    from deduped_step2
    where row_num = 1
)

select
    *
from final

{% endmacro %}

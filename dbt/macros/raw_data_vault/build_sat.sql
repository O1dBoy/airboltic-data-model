{% macro build_sat(source_table, hub_name, key_, effective_datetime_column, columns, incremental_column) %}

{% set columns = columns | map("lower") | list %}

with base as (
    select
        {{ concat_and_hash(include=[key_]) }} as hub__{{ hub_name }}__key,
        {% for column in columns %}
        {{ column }} as {{ column }},
        {% endfor %}
        topic as _source,
        current_timestamp as _load_datetime,
        {{ effective_datetime_column }} as _effective_datetime,
        {{ concat_and_hash(include=columns) }} as _record_hash,
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
        from {{ this }} s
        where 1 = 1
            and b.hub__{{ hub_name }}__key = s.hub__{{ hub_name }}__key
            and b._record_hash = s._record_hash
            and b._effective_datetime = s._effective_datetime
    )
    {% endif %}
),

deduped_step2 as (
    select
        *,
        row_number() over (
            partition by hub__{{ hub_name }}__key, _record_hash, _effective_datetime
            order by _effective_datetime desc
        ) as row_num
    from deduped_step1
),

final as (
    select
        hub__{{ hub_name }}__key,
        {% for column in columns %}
            {{ column }},
        {% endfor %}
        _source,
        _load_datetime,
        _effective_datetime,
        _record_hash,
        _incremental_date
    from deduped_step2
    where row_num = 1
)

select
    *
from final

{% endmacro %}

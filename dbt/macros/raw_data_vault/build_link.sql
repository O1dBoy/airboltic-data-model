{% macro build_link(source_table, hub_names, keys, incremental_column) %}

with base as (
    select
        {{ concat_and_hash(include=keys) }} as link__{{ hub_names | join("__") }}__key,
        {% for i in range(hub_names | length) %}
        {{ keys[i] }} as hub__{{ hub_names[i] }}__key,
        {% endfor %}
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
        from {{ this }} l
        where b.link__{{ hub_names | join("__") }}__key = l.link__{{ hub_names | join("__") }}__key
    )
    {% endif %}
),

deduped_step2 as (
    select
        *,
        row_number() over (
            partition by link__{{ hub_names | join("__") }}__key
            order by _load_datetime desc
        ) as row_num
    from deduped_step1
),

final as (
    select
        link__{{ hub_names | join("__") }}__key,
        {% for i in range(hub_names | length) %}
        hub__{{ hub_names[i] }}__key,
        {% endfor %}
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

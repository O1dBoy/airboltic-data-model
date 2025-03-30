{% macro incremental_filter(execution_start_date, execution_end_date, date_column='imported_at')  %}
    {% if execution_end_range %}
        {{ date_column }} >= {{ execution_start_date}} and
        {{ date_column }} < {{ execution_end_date}}
    {% else %}
        {{ date_column }} >= {{ execution_start_date}}
    {% endif %}
{% endmacro %}

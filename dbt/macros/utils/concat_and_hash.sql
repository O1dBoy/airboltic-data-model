{% macro concat_and_hash(include = [], separator = '~') %}

{#
"""
This macro takes a list of columns and concatenates them into a single string, with the
list of elements separated by the passed separator. The concatenated string is then hashed using
the SHA1 algorithm.

Arguments:
----------
include: the list of columns to concatenate. Example: ['col1', 'col2'].
separator: the separator to use between the columns. Default is '~'.
"""
#}

{% set include = include | map("lower") | list %}

sha1(
    concat(
        {% for col in include %}
        coalesce(cast({{ col }} as string), '') {% if not loop.last %}, '{{ separator }}',{% endif %}
        {% endfor %}
    )
)

{% endmacro %}

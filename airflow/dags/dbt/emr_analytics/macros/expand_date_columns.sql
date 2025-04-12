{% macro expand_date_columns(column_name, column_date_string_alias=none, column_date_alias=none) %}
    {{ column_name }} as {{ column_date_string_alias if column_date_string_alias is not none else column_name ~ '_date_string' }},
    {{ dbt.safe_cast(column_name, api.Column.translate_type("date")) }} as {{ column_date_alias if column_date_alias is not none else column_name ~ '_date' }}
{%- endmacro %}

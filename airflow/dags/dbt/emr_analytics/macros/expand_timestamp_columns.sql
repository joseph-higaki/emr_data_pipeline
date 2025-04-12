{% macro expand_timestamp_columns(column_name) %}
    {{ column_name }} as {{ column_name }}_at_string,
    {{ dbt.safe_cast(column_name, api.Column.translate_type("timestamp")) }} as {{ column_name }}_at,                
    {{ dbt.safe_cast(
            date_trunc("day", dbt.safe_cast(column_name, api.Column.translate_type("timestamp"))), 
            api.Column.translate_type("date"))  
    }} as {{ column_name }}_date
{%- endmacro %}

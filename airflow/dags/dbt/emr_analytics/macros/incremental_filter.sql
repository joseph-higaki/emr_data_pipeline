{% macro incremental_filter(column_name) %}
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run    
    where {{ column_name }} > (select coalesce(max({{ column_name }}), '1800-01-01') from {{ this }})
    {% endif %}
{%- endmacro %}
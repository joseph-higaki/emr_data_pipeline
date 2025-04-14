{% macro calculate_patient_age(patient_birth_date_column, reference_date_column) %}
    {{ datediff(patient_birth_date_column, reference_date_column, 'month') }} / 12
{% endmacro %}
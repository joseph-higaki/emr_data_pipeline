with

source as (

    select * from {{ ref('int_patient_medications') }}

)
select distinct
    medication_code,
    medication_description
from source

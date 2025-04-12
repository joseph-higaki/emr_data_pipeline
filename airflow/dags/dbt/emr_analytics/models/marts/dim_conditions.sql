
with

source as (

    select * from {{ ref('int_patient_conditions') }}

)
select distinct 
    condition_code,
    condition_description,
    condition_description_type
from source

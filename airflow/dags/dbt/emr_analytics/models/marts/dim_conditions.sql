
with

source as (

    select * from {{ ref('int_patient_conditions') }}

)
select 
    condition_code,
    condition_description
from source

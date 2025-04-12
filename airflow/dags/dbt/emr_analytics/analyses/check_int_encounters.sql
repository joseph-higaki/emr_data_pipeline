with a as (
    select * 
    from {{ ref('int_patient_encounters') }}    
    limit 10
)
select * 
from a
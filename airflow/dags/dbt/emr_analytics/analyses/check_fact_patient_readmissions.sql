with encounter as(
    select * 
    
    from {{ ref('fact_patient_encounter_readmissions') }} e
    where 1=1

)
, result as ( 
select *
from encounter

)
select 
    encounter_class,
    sum(readmission_encounter_count) as readmission_encounter_count,
    sum(encounter_count) as encounter_count

from result  
group by 
    encounter_class
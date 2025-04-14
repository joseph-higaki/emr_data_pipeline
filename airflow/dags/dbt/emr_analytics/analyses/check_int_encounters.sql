with a as (
    select * 
    from {{ ref('fact_patient_encounters') }}    
 
)
, result as (
select * 
from a
where 1=1 
 and encounter_class not in('Ambulatory', 'Outpatient')
--and encounter_class in('Emergency', 'Inpatient')
and  days_between_encounters < 8
limit 100
)
select * 
from result 

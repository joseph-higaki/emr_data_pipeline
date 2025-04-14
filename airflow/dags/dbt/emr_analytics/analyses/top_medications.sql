with patient_medication as  (
    select * 
    from {{ ref('fact_top_medications_by_patient_age_group') }}
    
),
grouped as (
    select 
    medication_description,
    -- age_group_id,
    -- age_group_description,
    sum(total_dispenses) as total_dispenses
    from  patient_medication 
    group by 
    medication_description
    -- ,
    -- age_group_id,
    -- age_group_description
    -- order by age_group_id
    limit 200
)
select *
from grouped 
order by 
-- age_group_id,
 total_dispenses desc

with a as (
    select * 
    from {{ ref('fact_patient_encounters_by_month') }} e
    limit 100
) 
select *
from a
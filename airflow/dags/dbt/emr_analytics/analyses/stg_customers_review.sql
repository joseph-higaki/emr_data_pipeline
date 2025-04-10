with a as (
select  *
from {{ ref('stg_emr__patients') }} 
order by patient_id
limit 8
) 
select * from a
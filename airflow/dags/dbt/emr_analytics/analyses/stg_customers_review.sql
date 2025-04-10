with a as (
select  patient_id, first_name, last_name, ingested_at
from {{ ref('stg_emr__patients') }} 
order by patient_id
limit 4
) 
select * from a
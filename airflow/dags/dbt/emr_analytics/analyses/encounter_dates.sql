select 
    min(e.start_date) as min_start_date,
    max(e.start_date) as max_start_date
       
       
from {{ ref('int_patient_encounters') }} e

with encounter as(
    select * 
    
    from {{ ref('int_patient_encounters') }}

)
, encounter_and_past_encounters as ( 
    select
        current_e.*,
        {{ datediff("past_e.start_at",  "current_e.start_at", "second") }} / 60 /60 /24 as days_between_encounters,
        row_number() over (partition by current_e.encounter_id order by current_e.start_at, past_e.start_at) as past_encounter_number
    from encounter current_e
    left join encounter past_e     
        on current_e.patient_id = past_e.patient_id
            and current_e.patient_id = past_e.patient_id
            and current_e.organization_id = past_e.organization_id
            and current_e.encounter_class = past_e.encounter_class            
            and current_e.start_at > past_e.start_at    
)
, result as(
    select * 
    from encounter_and_past_encounters
    where past_encounter_number = 1    
)

select
    *
from result

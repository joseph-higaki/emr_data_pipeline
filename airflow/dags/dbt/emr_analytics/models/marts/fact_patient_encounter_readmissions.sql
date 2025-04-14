{{
    config(
        materialized='table'
    )
}}

with encounter as(
    select *     
    from {{ ref('fact_patient_encounters') }}
)
, result as ( 
    select
        e.encounter_class,
        e.start_date,
        sum(case when e.days_between_encounters <= 7 then 1 else 0 end) as readmission_encounter_count,
        count(1) as encounter_count
    from encounter e
    group by 
        e.encounter_class,
        e.start_date
)

select * 
from result
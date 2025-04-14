{{
    config(
        materialized='view'        
    )
}}
with

condition as (

    select * from {{ ref('int_patient_conditions') }}

),
encounter as (
    select * from {{ ref('int_patient_encounters') }}
)
select
    c.*,
    e.encounter_class
from condition c
inner join encounter e
    on c.encounter_id = e.encounter_id


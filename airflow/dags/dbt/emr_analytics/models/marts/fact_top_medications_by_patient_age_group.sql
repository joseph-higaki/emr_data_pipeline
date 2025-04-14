{{
    config(
        materialized='view'        
    )
}}
with medications as (
    select 
        m.medication_code,
        m.medication_description,
        m.age_group_id,
        m.age_group_name,
        m.age_group_description,
        m.start_date,        
        m.dispenses
    from {{ ref('int_patient_medications') }}  m
),
grouped_medications as (
    select
        m.medication_code,
        m.medication_description,
        m.age_group_id,
        m.age_group_name,
        m.age_group_description,
        m.start_date,
        sum(m.dispenses) as total_dispenses      
    from medications m
    group by 
        m.medication_code,
        m.medication_description,
        m.age_group_id,
        m.age_group_name,
        m.age_group_description,
        m.start_date
        
)
select *
from grouped_medications
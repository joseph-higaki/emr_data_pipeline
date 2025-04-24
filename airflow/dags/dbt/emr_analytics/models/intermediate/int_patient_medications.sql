{{
    config(
        materialized='incremental',
        unique_key='patient_medication_id'
    )
}}
with source as (
    select * from {{ ref('stg_emr__medications') }}
    {{ incremental_filter('ingested_at') }}    
)
, with_rn as (
    select *,
        row_number() over (
            partition by patient_medication_id
            order by ingested_at desc
        ) as rn
    from source
)
, dedup as (
    select 
    {{ dbt_utils.star(from=ref('stg_emr__medications')) }}
    from with_rn
    where rn = 1
),
patients as (
    select
        patient_id,
        birth_date
    from {{ ref('int_patients') }}
),
medications as (
    select
        *
    from dedup
),
patients_age_medication_duration as (
    select        
        m.*,
        {{ calculate_patient_age('p.birth_date', 'm.start_at') }} as patient_age_at_medication_start_years, 
        {{ calculate_patient_age('p.birth_date', 'm.stop_at') }} as patient_age_at_medication_stop_years,         
        {{ datediff("m.start_at", "m.stop_at", "second") }} /60/60 as medication_duration_hours
    from medications m
    left join patients p
        on m.patient_id = p.patient_id
),
medication_with_age_group as (
    select 
        m.*,
        ag.age_group_id,
        ag.age_group_name,
        ag.description as age_group_description
    from patients_age_medication_duration m
    join {{ ref('age_group')}} ag 
        on ag.min_age < m.patient_age_at_medication_start_years 
        and m.patient_age_at_medication_start_years <= ag.max_age            
)
select * 
from medication_with_age_group

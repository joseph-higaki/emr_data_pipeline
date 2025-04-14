{{
    config(
        materialized='incremental',
        unique_key=['encounter_id', 'condition_code'],
    )
}}
with

source as (

    select * from {{ ref('stg_emr__conditions') }}

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run    
    where ingested_at > (select coalesce(max(ingested_at), '1800-01-01') from {{ this }})

    {% endif %}

),
conditions as (
    select
        patient_id,
        encounter_id,
        condition_code,        
        case 
            when condition_description like '%(%)'
            then regexp_substr(condition_description, '\\(([^)]+)\\)', 1, 1)
            else ''
        end as condition_description_type,
        case
            when condition_description like '%(%)'
            then trim(regexp_replace(condition_description, '\\s*\\([^)]*\\)', ''))
            else condition_description
        end as condition_description,
        start_date,    
        stop_date,
        ingested_at
    from source
),
patients as (
    select
        patient_id,
        birth_date
    from {{ ref('int_patients') }}
),
patients_age_condition_duration as (
    select 
        c.*,
        {{ calculate_patient_age('p.birth_date', 'c.start_date') }} as patient_age_at_condition_start_years, 
        {{ calculate_patient_age('p.birth_date', 'c.stop_date') }} as patient_age_at_condition_stop_years,         
        {{ datediff("c.start_date", "c.stop_date", "second") }} /60/60 as condition_duration_hours
    from conditions c
    left join patients p
        on c.patient_id = p.patient_id
),
condition_with_age_group as (
    select 
        c.*,
        ag.age_group_id,
        ag.age_group_name,
        ag.description as age_group_description
    from patients_age_condition_duration c
    join {{ ref('age_group')}} ag 
        on ag.min_age < c.patient_age_at_condition_start_years 
        and c.patient_age_at_condition_start_years <= ag.max_age  
)
select * 
from condition_with_age_group


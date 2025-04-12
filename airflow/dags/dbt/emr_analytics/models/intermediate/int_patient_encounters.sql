{{
    config(
        materialized='incremental',
        unique_key='encounter_id'         
    )
}}
with

source as (

    select * from {{ ref('stg_emr__encounters') }}

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run    
    where ingested_at > (select coalesce(max(ingested_at), '1800-01-01') from {{ this }})

    {% endif %}

),
patients as (
    select
        patient_id,
        birth_date
    from {{ ref('int_patients') }}
)
select
    e.encounter_id,    
    e.start_at,
    e.start_date,    
    e.stop_at,
    e.stop_date,    
    {{ datediff("p.birth_date", "e.start_at", "month") }} / 12 as patient_age_at_encounter_start_years,
    {{ datediff("p.birth_date", "e.stop_at", "month") }} / 12 as patient_age_at_encounter_stop_years,
    {{ datediff("e.start_at", "e.stop_at", "second") }} /60/60 as encounter_duration_hours,
    e.patient_id,
    e.organization_id,
    e.provider_id,
    e.payer_id,
    e.encounter_class,
    e.encounter_code,
    e.encounter_description,
    e.base_encounter_cost,
    e.total_claim_cost,
    e.payer_coverage,
    e.reason_code,
    e.reason_description,
    e.ingested_at,
    p.birth_date as patient_birth_date
from source e
left join patients p
    on e.patient_id = p.patient_id


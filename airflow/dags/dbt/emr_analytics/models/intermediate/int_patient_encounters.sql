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

)
select
    encounter_id,
    start_at_string,
    start_at,
    start_date,
    stop_at_string,
    stop_at,
    stop_date,
    patient_id,
    organization_id,
    provider_id,
    payer_id,
    encounter_class,
    encounter_code,
    encounter_description,
    base_encounter_cost,
    total_claim_cost,
    payer_coverage,
    reason_code,
    reason_description,
    ingested_at
from source
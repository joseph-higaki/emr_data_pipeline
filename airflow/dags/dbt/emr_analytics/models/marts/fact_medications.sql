{{
    config(
        materialized='incremental',
        unique_key=['patient_id', 'encounter_id'],
    )
}}
with

source as (

    select * from {{ ref('int_patient_medications') }}

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run    
    where ingested_at > (select coalesce(max(ingested_at), '1800-01-01') from {{ this }})

    {% endif %}

)
select
    patient_id,
    payer_id,
    encounter_id,
    medication_code,
    base_cost,
    payer_coverage,
    dispenses,
    total_cost,
    start_at,
    start_date,
    stop_at,
    stop_date,
    reason_code,
    ingested_at
from source

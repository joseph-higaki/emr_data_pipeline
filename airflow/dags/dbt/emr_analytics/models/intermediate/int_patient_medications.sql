{{
    config(
        materialized='incremental',
        unique_key='patient_medication_id'
    )
}}
with

source as (

    select * from {{ ref('stg_emr__medications') }}

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run    
    where ingested_at > (select coalesce(max(ingested_at), '1800-01-01') from {{ this }})

    {% endif %}

)

select
    {{ dbt_utils.generate_surrogate_key(['encounter_id', 'medication_code', 'start_at', 'stop_at']) }} as patient_medication_id,    
    encounter_id,
    patient_id,
    payer_id,
    start_at,
    start_date,
    stop_at,
    stop_date,
    medication_code,    
    medication_description,
    base_cost,
    payer_coverage,
    dispenses,
    total_cost,        
    reason_code,
    reason_description,
    ingested_at
from source

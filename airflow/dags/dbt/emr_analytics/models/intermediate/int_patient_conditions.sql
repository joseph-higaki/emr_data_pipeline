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

)

select
    patient_id,
    encounter_id,
    condition_code,
    condition_description,    
    start_date,    
    stop_date,
    ingested_at
from source

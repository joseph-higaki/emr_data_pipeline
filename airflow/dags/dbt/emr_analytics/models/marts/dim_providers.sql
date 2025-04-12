{{
    config(
        materialized='incremental',
        unique_key='provider_id'         
    )
}}
with

source as (

    select * from {{ ref('int_providers') }}

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run    
    where ingested_at > (select coalesce(max(ingested_at), '1800-01-01') from {{ this }})

    {% endif %}

)
select * from source

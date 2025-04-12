{{
    config(
        materialized='incremental',
        unique_key='organization_id'         
    )
}}
with

source as (

    select * from {{ ref('int_organizations') }}

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run    
    where ingested_at > (select coalesce(max(ingested_at), '1800-01-01') from {{ this }})

    {% endif %}

)
select * from source

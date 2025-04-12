{{
    config(
        materialized='incremental',
        unique_key='organization_id'         
    )
}}
with

source as (

    select * from {{ ref('stg_emr__organizations') }}


)

select
    organization_id,
    name,
    address,
    city,
    state,
    zip_code,
    lat,
    lon,
    phone,
    revenue,
    ingested_at
from source
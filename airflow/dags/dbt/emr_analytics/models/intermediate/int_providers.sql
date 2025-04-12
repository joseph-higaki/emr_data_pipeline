{{
    config(
        materialized='incremental',
        unique_key='provider_id'         
    )
}}
with

source as (

    select * from {{ ref('stg_emr__providers') }}

)

select
    provider_id,
    organization_id,
    name,
    gender,
    speciality,
    address,
    city,
    state,
    zip_code,
    lat,
    lon,
    ingested_at
from source

{{
    config(
        materialized='incremental',
        unique_key='provider_id'         
    )
}}
with

source as (

    select * from {{ ref('stg_emr__providers') }}
    {{ incremental_filter('ingested_at') }}    
)
, with_rn as (
    select *,
        row_number() over (
            partition by provider_id
            order by ingested_at desc
        ) as rn
    from source
)
, dedup as (
    select 
    {{ dbt_utils.star(from=ref('stg_emr__providers')) }}
    from with_rn
    where rn = 1
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
from dedup

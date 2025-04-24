{{
    config(
        materialized='incremental',
        unique_key='organization_id'         
    )
}}
with
source as (
    select * from {{ ref('stg_emr__organizations') }}    
    {{ incremental_filter('ingested_at') }}    
)
, with_rn as (
    select *,
        row_number() over (
            partition by organization_id
            order by ingested_at desc
        ) as rn
    from source
)
, dedup as (
    select 
    {{ dbt_utils.star(from=ref('stg_emr__organizations')) }}
    from with_rn
    where rn = 1
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
from dedup
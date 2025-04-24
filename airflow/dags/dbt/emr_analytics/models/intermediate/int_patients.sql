{{
    config(
        materialized='incremental',
         unique_key='patient_id'         
    )
}}
with

source as (

    select * from {{ ref('stg_emr__patients') }}
    {{ incremental_filter('ingested_at') }}    
)
, with_rn as (
    select *,
        row_number() over (
            partition by patient_id
            order by ingested_at desc
        ) as rn
    from source
)
, dedup as (
    select 
    {{ dbt_utils.star(from=ref('stg_emr__patients')) }}
    from with_rn
    where rn = 1
)
select * from dedup
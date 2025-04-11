{{
    config(
        materialized='incremental',
        unique_key='organization_id'         
    )
}}
with

source as (

    select * from {{ source('emr', 'raw_organizations') }}

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run    
    where ingested_at > (select coalesce(max(ingested_at), '1800-01-01') from {{ this }})

    {% endif %}

),

renamed as (
    select
        id as organization_id,
        name,
        address,
        city,
        state,
        zip as zip_code,
        {{ dbt.safe_cast("lat", api.Column.translate_type("numeric")) }} as lat,
        {{ dbt.safe_cast("lon", api.Column.translate_type("numeric")) }} as lon,
        phone,
        {{ dbt.safe_cast("revenue", api.Column.translate_type("numeric")) }} as revenue,
        {{ dbt.safe_cast("utilization", api.Column.translate_type("numeric")) }} as utilization,
        ingested_at
    from source
)

select * from renamed

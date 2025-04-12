with

source as (

    select * from {{ source('emr', 'raw_providers') }}

),

renamed as (
    select
        id as provider_id,
        organization as organization_id,
        name,
        gender,
        speciality,
        address,
        city,
        state,
        zip as zip_code,        
        {{ dbt.safe_cast("lat", api.Column.translate_type("numeric")) }} as lat,
        {{ dbt.safe_cast("lon", api.Column.translate_type("numeric")) }} as lon,        
        ingested_at
    from source
)

select * from renamed

with

source as (

    select * from {{ source('emr', 'raw_patients') }}

),

renamed as (

    select

        ----------  ids
        id as patient_id,

        ---------- dates
        birthdate as birthdate_string,
        {{ dbt.safe_cast("birthdate", api.Column.translate_type("date")) }} as birth_date,                
        deathdate as deathdate_string,        
        {{ dbt.safe_cast("deathdate", api.Column.translate_type("date")) }} as death_date,        
        
        ---------- text
        ssn as social_security_number,
        drivers as drivers_license,
        passport as passport_number,
        prefix,
        first as first_name,
        last as last_name,
        suffix,
        maiden as maiden_name,
        marital as marital_status,
        race,
        ethnicity,
        gender,
        birthplace as birthplace,
        address
        city,
        state,
        county,
        fips as fips_code,
        zip as zip_code,

        #--------- numbers
        {{ dbt.safe_cast("lat", api.Column.translate_type("numeric")) }} as lat,
        {{ dbt.safe_cast("lon", api.Column.translate_type("numeric")) }} as lon,
        {{ dbt.safe_cast("healthcare_expenses", api.Column.translate_type("numeric")) }} as healthcare_expenses,
        {{ dbt.safe_cast("healthcare_coverage", api.Column.translate_type("numeric")) }} as healthcare_coverage,
        {{ dbt.safe_cast("income", api.Column.translate_type("numeric")) }} as income,         
        ingested_at
    from source

)

select * from renamed
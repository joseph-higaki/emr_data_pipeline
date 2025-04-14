with

source as (

    select * from {{ source('emr', 'raw_patients') }}

),

renamed as (

    select

        ----------  ids
        id as patient_id,

        ---------- dates
        {{ expand_date_columns('birthdate', 'birth_date_string', 'birth_date') }},   
        {{ expand_date_columns('deathdate', 'death_date_string', 'death_date') }},
        
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
        zip as zip_code,

        #--------- numbers
        {{ dbt.safe_cast("lat", api.Column.translate_type("numeric")) }} as lat,
        {{ dbt.safe_cast("lon", api.Column.translate_type("numeric")) }} as lon,
        {{ dbt.safe_cast("healthcare_expenses", api.Column.translate_type("numeric")) }} as healthcare_expenses,
        {{ dbt.safe_cast("healthcare_coverage", api.Column.translate_type("numeric")) }} as healthcare_coverage,        
        ingested_at
    from source

)

select * from renamed
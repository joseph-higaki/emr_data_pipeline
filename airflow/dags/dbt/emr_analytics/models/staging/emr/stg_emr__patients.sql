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
        {{ dbt.safe_cast("deathdate", api.Column.translate_type("date")) }} as deathdate,        
        
        ---------- text
        first as first_name,
        last as last_name,
        ssn as social_security_number,
        ingested_at
    from source

)

select * from renamed
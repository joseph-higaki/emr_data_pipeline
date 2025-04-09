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
        cast(birthdate as date) as birth_date,
        deathdate as deathdate_string,
        cast(deathdate as date) as death_date,

        ---------- text
        first as first_name,
        last as last_name,
        ssn as social_security_number
    from source

)

select * from renamed
with

source as (

    select * from {{ source('emr', 'raw_conditions') }}

),

renamed as (
    select        
        patient as patient_id,
        encounter as encounter_id,
        code as condition_code,
        description as condition_description,
        {{ expand_date_columns('start') }},        
        {{ expand_date_columns('stop') }},
        ingested_at
    from source
)

select * from renamed

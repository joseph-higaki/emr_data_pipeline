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
        start as start_at_string,
        {{ dbt.safe_cast("start", api.Column.translate_type("timestamp")) }} as start_at,
        {{ dbt.safe_cast("start", api.Column.translate_type("date")) }} as start_date,
        stop as stop_at_string,
        {{ dbt.safe_cast("stop", api.Column.translate_type("timestamp")) }} as stop_at,
        {{ dbt.safe_cast("stop", api.Column.translate_type("date")) }} as stop_date,
        ingested_at
    from source
)

select * from renamed

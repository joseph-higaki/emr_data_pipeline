with

source as (

    select * from {{ source('emr', 'raw_medications') }}

),

renamed as (
    select
        patient as patient_id,
        payer as payer_id,
        encounter as encounter_id,
        code as medication_code,
        description as medication_description,
        base_cost,
        payer_coverage,
        dispenses,
        totalcost as total_cost,
        start as start_at_string,
        {{ dbt.safe_cast("start", api.Column.translate_type("timestamp")) }} as start_at,                
        stop as stop_at_string,
        {{ dbt.safe_cast("stop", api.Column.translate_type("timestamp")) }} as stop_at,        
        reasoncode as reason_code,
        reasondescription as reason_description,
        ingested_at
    from source
),
date_extracted as (
    select 
        *,
        {{ dbt.safe_cast(date_trunc("day", "start_at"), api.Column.translate_type("date"))  }} as start_date,
        {{ dbt.safe_cast(date_trunc("day", "stop_at"), api.Column.translate_type("date"))  }} as stop_date
    from renamed

)
select * from date_extracted

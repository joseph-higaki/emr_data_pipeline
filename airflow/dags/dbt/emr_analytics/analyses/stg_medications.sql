with

source as (

    select * from {{ source('emr', 'raw_medications') }}
    where encounter = 'd6a83835-7bbf-5aed-ceb1-1039fa5417e3' 

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
        {{ dbt.safe_cast("start", api.Column.translate_type("date")) }} as start_date,
        stop as stop_at_string,
        {{ dbt.safe_cast("stop", api.Column.translate_type("timestamp")) }} as stop_at,
        {{ dbt.safe_cast("stop", api.Column.translate_type("date")) }} as stop_date,
        reasoncode as reason_code,
        reasondescription as reason_description,
        ingested_at
    from source
),
date_extracted as (
    select 
        *,
        {{ date_trunc("day", "start_at") }} as satart_date,
        {{ date_trunc("day", "stop_at") }} as stop_date
    from 

)
select * from date_extracted

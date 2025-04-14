with

source as (

    select * from {{ source('emr', 'raw_medications') }}
    -- where encounter = 'd6a83835-7bbf-5aed-ceb1-1039fa5417e3' 

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
grouped as (
    select 
        encounter_id,
        medication_code,
        start_at,
        stop_at,
        ingested_at,
        count(1) as count
    from renamed    
    group by 
        encounter_id,
        medication_code,
        start_at,
        stop_at,
        ingested_at
    having count(1) > 1
    limit 100
)
select * from grouped

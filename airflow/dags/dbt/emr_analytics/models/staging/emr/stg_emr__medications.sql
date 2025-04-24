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
        {{ dbt.safe_cast("base_cost", api.Column.translate_type("numeric")) }} as base_cost,
        {{ dbt.safe_cast("payer_coverage", api.Column.translate_type("numeric")) }} as payer_coverage,
        {{ dbt.safe_cast("dispenses", api.Column.translate_type("numeric")) }} as dispenses,
        {{ dbt.safe_cast("totalcost", api.Column.translate_type("numeric")) }} as total_cost,
        
        {{ expand_timestamp_columns('start') }},
        
        {{ expand_timestamp_columns('stop') }},

        reasoncode as reason_code,
        reasondescription as reason_description,
        ingested_at
    from source
),
with_surrogatee as(
    select 
        {{ dbt_utils.generate_surrogate_key(['encounter_id', 'medication_code', 'start_at', 'stop_at', 'base_cost', 'dispenses']) }} as patient_medication_id,
        *
    from renamed
)
select * from with_surrogatee

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
        
        {{ expand_timestamp_columns('start') }},
        
        {{ expand_timestamp_columns('stop') }},

        reasoncode as reason_code,
        reasondescription as reason_description,
        ingested_at
    from source
)
select * from renamed

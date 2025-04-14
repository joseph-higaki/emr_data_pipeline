with

source as (

    select * from {{ source('emr', 'raw_encounters') }}

),

renamed as (
    select
        id as encounter_id,
        {{ expand_timestamp_columns('start') }},        
        {{ expand_timestamp_columns('stop') }},
        patient as patient_id,
        organization as organization_id,
        provider as provider_id,
        payer as payer_id,
        encounterclass as encounter_class,
        code as encounter_code,
        description as encounter_description,
        {{ dbt.safe_cast("base_encounter_cost", api.Column.translate_type("numeric")) }} as base_encounter_cost,
        {{ dbt.safe_cast("total_claim_cost", api.Column.translate_type("numeric")) }} as total_claim_cost,
        {{ dbt.safe_cast("payer_coverage", api.Column.translate_type("numeric")) }} as payer_coverage,        
        reasoncode as reason_code,
        reasondescription as reason_description,
        ingested_at
    from source
)
select * from renamed
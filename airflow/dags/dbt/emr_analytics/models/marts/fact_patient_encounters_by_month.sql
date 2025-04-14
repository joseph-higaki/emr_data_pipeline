{{
    config(
        materialized='table'
    )
}}
with encounter as(
    select * 
    
    from {{ ref('fact_patient_encounters') }}

)
, grouped_encounter as ( 
    select
        e.encounter_class,
        {{ date_trunc("month", "e.start_date") }} as encounter_month_timestamp,
        count(1) as encounter_count,
        sum(total_claim_cost) as total_claim_cost
    from encounter e
    group by 
        e.encounter_class,
        {{ date_trunc("month", "e.start_date") }}
)
, encounter_date_attributes as (
    select 
        *,
        extract(year from encounter_month_timestamp) as encounter_year_number,
        extract(month from encounter_month_timestamp) as encounter_month_number,
        {{ dbt_date.month_name("encounter_month_timestamp", short=true) }} as encounter_month_short_name,
        {{ dbt_date.month_name("encounter_month_timestamp", short=false) }} as encounter_month_full_name       
    from grouped_encounter
)
select * 
from encounter_date_attributes    

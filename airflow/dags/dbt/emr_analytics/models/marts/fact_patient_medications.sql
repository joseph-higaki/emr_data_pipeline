{{
    config(
        materialized='view'        
    )
}}
with

source as (

    select * from {{ ref('int_patient_medications') }}

)
select
    *
from source

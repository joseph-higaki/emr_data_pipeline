with medication as (
    select * from {{ source('emr', 'raw_medications') }}
    where encounter = 'b2e635b0-54d4-ea30-ea6a-a61e2ad46a64' 
),
renamed as (
    select * 
    from medication
    order by 
    limit 100
)
select* from renamed
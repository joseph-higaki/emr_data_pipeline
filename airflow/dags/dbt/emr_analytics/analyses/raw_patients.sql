with a as (
    select 
    {{ dbt.safe_cast("lat", api.Column.translate_type("numeric")) }} as latitude_numbe
    from {{ source('emr', 'raw_patients') }}
    order by id
    limit 10
)
select * from a
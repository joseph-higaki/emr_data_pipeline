select distinct
city
from {{ ref('dim_organizations') }}

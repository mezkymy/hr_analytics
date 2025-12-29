-- clean data from staging

select *
from {{ ref('staging') }}


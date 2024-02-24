with region_data as (
    select
        sp.stateprovinceid as region_id,
        sp.name as state_province_name,
        cr.name as country_region_name
    from {{ ref('stg_state_province') }} as sp
    join {{ ref('stg_country_region') }} as cr
    on sp.countryregioncode = cr.countryregioncode
)

select
    row_number() over (order by region_id) as region_sk,
    region_id,
    state_province_name,
    country_region_name
from region_data

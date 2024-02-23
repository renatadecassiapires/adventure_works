with source as (
    select
        sp.stateprovinceid as source_location_id,
        sp.name as location_name,
        cr.name as country_name
    from {{ ref('stg_state_province') }} sp
    join {{ ref('stg_country_region') }} cr
    on sp.countryregioncode = cr.countryregioncode
)

select
    row_number() over () as location_sk,
    source_location_id,
    location_name,
    country_name
from source

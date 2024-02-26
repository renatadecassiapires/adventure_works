with source_data as (
    select
        countryregioncode
        , modifieddate
        , name as country_name
    from {{ source('sap_adw', 'countryregion') }}
)
select *
from source_data
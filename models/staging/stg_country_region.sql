with source_data as (
    select
        countryregioncode,
        name,
        modifieddate
    from {{ source('sap_adw', 'countryregion') }}
)
select *
from source_data
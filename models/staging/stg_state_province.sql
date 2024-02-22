with source_data as (
    select
        stateprovinceid,
        stateprovincecode,
        countryregioncode,
        isonlystateprovinceflag,
        name,
        territoryid,
        rowguid,
        modifieddate
    from {{ source('sap_adw', 'stateprovince') }}
)
select *
from source_data

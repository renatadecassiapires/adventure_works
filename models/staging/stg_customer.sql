with source_data as (
    select
        customerid,
        personid,
        storeid,
        territoryid,
        rowguid,
        modifieddate
    from {{ source('sap_adw', 'customer') }}
)
select *
from source_data
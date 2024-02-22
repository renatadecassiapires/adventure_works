with source_data as (
    select
        businessentityid,
        name,
        salespersonid,
        demographics,
        rowguid,
        modifieddate
    from {{ source('sap_adw', 'store') }} 
)
select *
from source_data

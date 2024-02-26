with source_data as (
    select
        businessentityid
        , name as storename
        , salespersonid
        , modifieddate
    from {{ source('sap_adw', 'store') }}
)
select *
from source_data	
with source_data as (
    select
        salesreasonid,
        name,
        reasontype,
        modifieddate
    from {{ source('sap_adw', 'salesreason') }}
)
select *
from source_data

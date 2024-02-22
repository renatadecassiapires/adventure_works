with source_data as (
    select
        salesorderid,
        salesreasonid,
        modifieddate
    from {{ source('sap_adw', 'salesorderheadersalesreason') }} 
)
select *
from source_data

with stg_store as (
    select
        businessentityid
        , name as storename
        , salespersonid
        , modifieddate
    from {{ source('sap_adw', 'store') }} 
)
select *
from stg_store	


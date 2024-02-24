{{ config(materialized='table') }}

with source_data as (
    select
        a.addressid,
        a.stateprovinceid,
        a.city,
        a.addressline1,
        a.addressline2,
        a.postalcode,
        a.spatiallocation,
        a.rowguid,
        a.modifieddate
    from {{ source('sap_adw', 'address') }} a
)

select *
from source_data

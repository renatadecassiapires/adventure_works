with source_data as (
    select
        salesorderid,
        salesorderdetailid,
        carriertrackingnumber,
        orderqty,
        productid,
        specialofferid,
        unitprice,
        unitpricediscount,
        rowguid,
        modifieddate
    from {{ source('sap_adw', 'salesorderdetail') }}
)
select *
from source_data


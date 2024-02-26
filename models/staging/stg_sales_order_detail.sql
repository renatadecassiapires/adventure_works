WITH source_data AS (
    SELECT
        salesorderid,
        orderqty,
        salesorderdetailid,
        unitprice,
        specialofferid,
        modifieddate,
        rowguid,
        productid,
        unitpricediscount
    FROM `adventureworksdesafiolh.dbt_rpires.salesorderdetail`
)
SELECT *
FROM source_data

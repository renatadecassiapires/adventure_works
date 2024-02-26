WITH source_data AS (
    SELECT
        productid,
        name AS product_name,
        safetystocklevel,
        finishedgoodsflag,
        class,
        makeflag,
        productnumber,
        reorderpoint,
        modifieddate,
        rowguid,
        productmodelid,
        weightunitmeasurecode,
        standardcost,
        productsubcategoryid,
        listprice,
        daystomanufacture,
        productline,
        color,
        sellstartdate,
        weight AS product_weight
    FROM `adventureworksdesafiolh.dbt_rpires.product`
)
SELECT *
FROM source_data

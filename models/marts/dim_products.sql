with source_data as (
    select
        p.productid as source_product_id,
        p.name as product_name,
        p.productnumber,
        max(p.standardcost) as standardcost,
        max(p.listprice) as listprice,
        max(p.size) as size,
        max(p.color) as color,
        max(p.weight) as weight,
        max(p.productsubcategoryid) as productsubcategoryid,
        max(p.productmodelid) as productmodelid,
        max(p.sellstartdate) as sellstartdate,
        max(p.sellenddate) as sellenddate,
    from {{ ref('stg_product') }} p
    left join {{ ref('stg_sales_order_detail') }} sod on p.productid = sod.productid
    group by p.productid, p.name, p.productnumber
)

select
    row_number() over (order by source_product_id) as product_sk, 
    source_product_id,
    product_name,
    productnumber,
    standardcost,
    listprice,
    size,
    color,
    weight,
    productsubcategoryid,
    productmodelid,
    sellstartdate,
    sellenddate
from source_data

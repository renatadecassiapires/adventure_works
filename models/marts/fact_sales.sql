create or replace view `adventureworksdesafiolh`.`dbt_rpires`.`fact_sales` as
with 
sales_order_header as (
    select *
    from `adventureworksdesafiolh`.`dbt_rpires`.`stg_sales_order_header`
),
sales_order_detail as (
    select 
        distinct salesorderid,
        productid,
        orderqty,
        unitprice,
        unitpricediscount
    from `adventureworksdesafiolh`.`dbt_rpires`.`stg_sales_order_detail`
),
customers as (
    select 
        customer_sk,
        source_customer_id,
        fullname as customer_fullname
    from `adventureworksdesafiolh`.`dbt_rpires`.`dim_customers`
),
products as (
    select 
        product_sk,
        source_product_id,
        product_name
    from `adventureworksdesafiolh`.`dbt_rpires`.`dim_products`
),
address as (
    select
        addressid,
        city,
        postalcode,
        stateprovinceid  -- Adicionando a coluna stateprovinceid
    from `adventureworksdesafiolh`.`dbt_rpires`.`stg_address`
),
regions as (
    select 
        region_sk,
        region_id,
        state_province_name,
        country_region_name
    from `adventureworksdesafiolh`.`dbt_rpires`.`dim_region`
)
select
    soh.salesorderid,
    soh.orderdate,
    sod.productid,
    prod.product_name,
    sod.orderqty,
    sod.unitprice,
    sod.unitpricediscount,
    cust.source_customer_id,
    cust.customer_fullname,
    addr.city,
    addr.postalcode,
    loc.location_sk,
    loc.location_name,
    reg.region_sk,
    reg.state_province_name,
    reg.country_region_name,
    -- Cálculo do valor total do pedido (Quantidade * Preço Unitário - Desconto)
    (sod.orderqty * sod.unitprice) - sod.unitpricediscount as total_order_value
from 
    sales_order_header soh
left join 
    sales_order_detail sod on soh.salesorderid = sod.salesorderid
left join 
    customers cust on soh.customerid = cust.source_customer_id
left join 
    `adventureworksdesafiolh`.`dbt_rpires`.`dim_locations` loc on soh.shiptoaddressid = loc.location_sk
left join 
    products prod on sod.productid = prod.source_product_id
left join 
    address addr on soh.shiptoaddressid = addr.addressid
left join 
    regions reg on addr.stateprovinceid = reg.state_province_id


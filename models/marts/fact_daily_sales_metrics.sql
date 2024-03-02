{{ config(materialized='table', schema='aggregated_tables') }}

with daily_sales_data as (
    select
        soh.salesorderid,
        soh.orderdate,
        soh.shipmethod,
        f.unitprice,
        f.orderqty,
        f.totalproductcost,
        f.totaldue
    from {{ ref('fact_sales') }} f
    join {{ ref('stg_salesorderheader') }} soh
        on f.salesorderid = soh.salesorderid
),

daily_orders as (
    select
        orderdate,
        count(distinct salesorderid) as num_orders
    from daily_sales_data
    group by orderdate
),

daily_sales as (
    select
        orderdate,
        sum(totaldue) as total_sales
    from daily_sales_data
    group by orderdate
),

daily_average_order_value as (
    select
        orderdate,
        avg(totaldue) as avg_order_value
    from daily_sales_data
    group by orderdate
),

daily_average_products_per_order as (
    select
        orderdate,
        avg(orderqty) as avg_products_per_order
    from daily_sales_data
    group by orderdate
),

daily_conversion_rate as (
    select
        orderdate,
        count(distinct case when totaldue > 0 then salesorderid end) / count(distinct salesorderid) as conversion_rate
    from daily_sales_data
    group by orderdate
)

select
    d.orderdate,
    coalesce(do.num_orders, 0) as num_orders,
    coalesce(ds.total_sales, 0) as total_sales,
    coalesce(daov.avg_order_value, 0) as avg_order_value,
    coalesce(dappo.avg_products_per_order, 0) as avg_products_per_order,
    coalesce(dcr.conversion_rate, 0) as conversion_rate
from (
    select distinct orderdate from daily_sales_data
) d
left join daily_orders do on d.orderdate = do.orderdate
left join daily_sales ds on d.orderdate = ds.orderdate
left join daily_average_order_value daov on d.orderdate = daov.orderdate
left join daily_average_products_per_order dappo on d.orderdate = dappo.orderdate
left join daily_conversion_rate dcr on d.orderdate = dcr.orderdate;

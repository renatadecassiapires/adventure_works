{{ config(
    materialized='table',
    schema='aggregated_tables'
) }}

with sales_data as (
    select
        soh.salespersonid,
        soh.shiptoaddressid,
        f.salesorderid,
        cast(f.unitprice as numeric) as unitprice,  
        cast(f.orderqty as numeric) as orderqty,  
        DATE(f.orderdate) as orderdate
    from `adventureworksdesafiolh`.`dbt_rpires`.`fact_sales` f
    join `adventureworksdesafiolh`.`dbt_rpires`.`stg_salesorderheader` soh
        on f.salesorderid = soh.salesorderid
),

location_data as (
    select
        loc.shiptoaddress_sk as location_sk,
        loc.city_name,
        loc.state_name,
        loc.country_name
    from `adventureworksdesafiolh`.`dbt_rpires`.`dim_locations` loc
),

salesperson_data as (
    select
        p.businessentityid as salespersonid,
        p.firstname,
        p.lastname
    from `adventureworksdesafiolh`.`dbt_rpires`.`stg_person` p
),

aggregated_sales as (
    select
        sd.salespersonid,
        sd.orderdate,  
        ld.city_name,
        ld.state_name,
        ld.country_name,
        sp.firstname,
        sp.lastname,
        sum(sd.unitprice * sd.orderqty) as total_sales, 
        count(sd.salesorderid) as total_orders
    from sales_data sd
    join location_data ld
        on sd.shiptoaddressid = ld.location_sk
    join salesperson_data sp
        on sd.salespersonid = sp.salespersonid
    group by sd.salespersonid, sd.orderdate, ld.city_name, ld.state_name, ld.country_name, sp.firstname, sp.lastname
)

select
    salespersonid,
    firstname,
    lastname,
    city_name as cityname,
    state_name as statename,
    country_name as countryname,
    total_sales as totalsales,
    total_orders as totalorders,
    orderdate
from aggregated_sales

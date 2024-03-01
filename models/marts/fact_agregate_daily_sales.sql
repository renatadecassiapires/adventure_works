{{ config(materialized='table', schema='aggregated_tables') }}

with sales_data as (
    select
        soh.salespersonid,
        soh.shiptoaddressid,
        f.salesorderid,
        f.unitprice,
        f.orderqty,
        f.orderdate
    from {{ ref('fact_sales') }} f
    join {{ ref('stg_salesorderheader') }} soh
        on f.salesorderid = soh.salesorderid
),

location_data as (
    select
        loc.shiptoaddress_sk as location_sk,
        loc.city_name,
        loc.state_name,
        loc.country_name
    from {{ ref('dim_locations') }} loc
),

salesperson_data as (
    select
        p.businessentityid as salespersonid,
        p.firstname,
        p.lastname
    from {{ ref('stg_person') }} p
),

aggregated_sales as (
    select
        sd.salespersonid,
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
    group by sd.salespersonid, ld.city_name, ld.state_name, ld.country_name, sp.firstname, sp.lastname
)

select
    salespersonid,
    firstname,
    lastname,
    city_name,
    state_name,
    country_name,
    total_sales,
    total_orders
    orderdate
from aggregated_sales

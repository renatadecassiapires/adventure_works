{{ config(
    materialized='table',
    schema='aggregated_tables'
) }}

with salesperson_region_sales as (
    select
        s.salesperson_id,
        l.region_id,
        sum(f.unitprice * f.orderqty) as total_sales
    from {{ ref('fact_sales') }} f
    join {{ ref('stg_salesorderheader') }} s on f.salesperson_id = s.salesperson_id
    join {{ ref('dim_locations') }} l on f.location_id = l.location_id
    group by s.salesperson_id, l.region_id
)

select
    salesperson_id,
    region_id,
    total_sales
from salesperson_region_sales
order by salesperson_id, region_id



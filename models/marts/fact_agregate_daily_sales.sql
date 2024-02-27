{{ config(
    materialized='table',
    schema='aggregated_tables'
) }}

with sales_data as (
    select
        region_id,
        salesperson_id,
        sum(unitprice * orderqty) as total_sales
    from {{ ref('fact_sales') }}
    group by region_id, salesperson_id
)

select
    region_id,
    salesperson_id,
    total_sales
from sales_data
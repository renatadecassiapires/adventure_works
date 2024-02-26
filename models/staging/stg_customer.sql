with source_data as (
    select
        customerid
        , personid
        , storeid
        , territoryid
    from `adventureworksdesafiolh.dbt_rpires.customer` -- Replace with your actual project and dataset name
)
select *
from source_data

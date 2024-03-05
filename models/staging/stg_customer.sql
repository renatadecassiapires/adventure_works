with source_data as (
    select
        customerid
        , personid
        , storeid
        , territoryid
    from `adventureworksdesafiolh.dbt_rpires.customer` 
)
select *
from source_data

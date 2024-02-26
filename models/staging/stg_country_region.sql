with source_data as (
    select
        countryregioncode
        , modifieddate
        , name as country_name
    from `adventureworksdesafiolh.dbt_rpires.countryregion`
select *
from source_data

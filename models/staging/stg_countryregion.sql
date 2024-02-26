-- models/staging/stg_countryregion.sql

create or replace view `adventureworksdesafiolh`.`dbt_rpires`.`stg_countryregion`
OPTIONS()
as (
    select
        countryregioncode,
        modifieddate,
        name as country_name
    from `adventureworksdesafiolh.dbt_rpires.countryregion`
);

with source_data as (
    select
        creditcardid
        , cardtype
        , expyear
        , modifieddate
        , expmonth
        , cardnumber
    from `adventureworksdesafiolh.dbt_rpires.creditcard`
)
select *
from source_data

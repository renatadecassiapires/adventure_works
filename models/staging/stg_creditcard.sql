with source_data as (
    select
        creditcardid,
        cardtype,
        cardnumber,
        expmonth,
        expyear,
        modifieddate
    from {{ source('sap_adw', 'creditcard') }}
)
select *
from source_data

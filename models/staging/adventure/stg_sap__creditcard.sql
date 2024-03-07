
with
    fonte_creditcard as (
        select 
        cast(creditcardid as int) as id_cartao
        ,cast(cardtype as string) as nome_bandeira_cartao
      --  ,cast(cardnumber as tipo) as nome
       -- ,cast(expmonth as tipo) as nome
       -- ,cast(expyear as tipo) as nome
      --  ,cast(modifieddate as tipo) as nome
        from {{ source('sap', 'creditcard') }}
    )

select *
from fonte_creditcard
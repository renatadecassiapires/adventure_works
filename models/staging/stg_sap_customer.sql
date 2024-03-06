
with
    fonte_customer as (
        select 
        cast(customerid as int) as id_cliente
        , cast(personid as int) as id_pessoa
        , cast(storeid as int) as id_loja
        , cast(territoryid as int) as id_territorio
        -- , cast(rowguid as tipo) as nome
        -- , cast(modifieddate as tipo) as nome
        from {{ source('sap', 'customer') }}
    )

select *
from fonte_customer



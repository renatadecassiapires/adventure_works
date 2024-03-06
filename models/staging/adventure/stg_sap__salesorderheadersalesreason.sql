with
    fonte_salesorderheadersalesreason as (
        select 
        cast(salesorderid as int) as id_pedido
        , cast(salesreasonid as int) as id_motivo
       --, cast(modifieddate as tipo) as nome
        from {{ source('sap', 'salesorderheadersalesreason') }}
    )
select *
from fonte_salesorderheadersalesreason


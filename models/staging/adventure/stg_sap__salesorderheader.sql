with
    fonte_salesorderheader as (
        select 
        cast (salesorderid as int) as id_pedido
        --, cast (revisionnumber as tipo) as nome
        , cast (orderdate as timestamp) as data_pedido
        --, cast (duedate as tipo) as nome
        --, cast (shipdate as tipo) as nome
        , cast (status as int) as id_status
       -- , cast (onlineorderflag as tipo) as nome
        --, cast (purchaseordernumber as tipo) as nome
        -- , cast (accountnumber as tipo) as nome
        , cast (customerid as int) as id_cliente
        , cast (salespersonid as int) as  id_vendedor
        , cast (territoryid as int) as id_territorio
       -- , cast (billtoaddressid as tipo) as nome
        , cast (shiptoaddressid as int) as endereco_destino
        --, cast (shipmethodid as tipo) as nome
        , cast (creditcardid as int) as id_cartao
        , cast (creditcardapprovalcode as string) as id_compra_aprovada
        -- , cast (currencyrateid as tipo) as nome
        , cast (subtotal as decimal) as subtotal_compra
        --, cast (taxamt as tipo) as nome
       -- , cast (freight as tipo) as nome
        , cast (totaldue as decimal) as total_compra
       -- , cast (comment as tipo) as nome
       -- , cast (rowguid as tipo) as nome
      --  , cast (modifieddate as tipo) as nome
        from {{ source('sap', 'salesorderheader') }}
     )

select *
from fonte_salesorderheader



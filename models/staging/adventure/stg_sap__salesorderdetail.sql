with
    fonte_salesorderdetail as (
        select 
        cast(salesorderid as int) as id_pedido
        , cast(salesorderdetailid as int) as id_detalhe_pedido
       --  , cast(carriertrackingnumber as tipo) as nome
        , cast(orderqty as int) as quantidade_pedido
        , cast(productid as int) as id_produto
        , cast(specialofferid as int) as id_oferta
        , cast(unitprice as decimal) as preco_unitario
        , cast(unitpricediscount as decimal) as desconto_preco_unitario
        --, cast(rowguid as tipo) as nome
        --, cast(modifieddate as tipo) as nome
        from {{ source('sap', 'salesorderdetail') }}
    )

select *
from fonte_salesorderdetail
order by id_pedido

with 
    stg_salesorderheader as (
        select *
        from {{ ref('stg_sap__salesorderheader') }}
    )
    , stg_salesorderdetail as (
        select *
        from {{ ref('stg_sap__salesorderdetail') }}
    )

      , join_tabelas as (
        select 
              stg_salesorderheader.id_pedido
            , stg_salesorderheader.data_pedido
            , stg_salesorderheader.id_status
            , stg_salesorderheader.id_cliente
            , stg_salesorderheader.id_vendedor
            , stg_salesorderheader.id_territorio
            , stg_salesorderheader.endereco_destino
            , stg_salesorderheader.id_cartao
            , stg_salesorderheader.id_compra_aprovada
            , stg_salesorderheader.subtotal_compra
            , stg_salesorderheader.total_compra

            -- stg_salesorderdetail.id_pedido
            , stg_salesorderdetail.id_detalhe_pedido
            , stg_salesorderdetail.quantidade_pedido
            , stg_salesorderdetail.id_produto
            , stg_salesorderdetail.id_oferta
            , stg_salesorderdetail.preco_unitario
            , stg_salesorderdetail.desconto_preco_unitario

           

        from stg_salesorderdetail
        left join stg_salesorderheader on 
            stg_salesorderdetail.id_pedido = stg_salesorderheader.id_pedido
               )

     , criar_chave as (
        select 
           cast(id_pedido as string) || cast(id_produto as string) as sk_pedido_item
            , *
        from join_tabelas
    )

    select distinct *
    from criar_chave
with
    stg_sap__creditcard as (
        select *
        from {{ ref('stg_sap__creditcard') }}
    )

    , stg_sap__salesorderheader  as (
        select *
        from {{ ref('stg_sap__salesorderheader') }}
    )

  , join_tabelas as (
        select 
            stg_sap__creditcard.id_cartao
            , stg_sap__creditcard.nome_bandeira_cartao
    

   -- ,  stg_salesorderheader.id_pedido
   -- ,  stg_salesorderheader.data_pedido
   -- ,  stg_salesorderheader.id_status
   -- ,  stg_salesorderheader.id_cliente
   -- ,  stg_salesorderheader.id_vendedor
   -- ,  stg_salesorderheader.id_territorio
   -- ,  stg_salesorderheader.id_cartao
   -- ,  stg_salesorderheader.id_compra_aprovada
   -- ,  stg_salesorderheader.subtotal_compra
   -- ,  stg_salesorderheader.total_compra

    from stg_sap__creditcard
       -- left join stg_sap__creditcard on
        --    stg_sap__salesorderheader.id_cartao = stg_sap__creditcard.id_cartao 

       )

    , criar_chave as (
        select
            row_number() over (order by id_cartao) as pk_pagamento
            , *
        from join_tabelas
    )

    select *
    from criar_chave
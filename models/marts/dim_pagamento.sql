with
    stg_creditcard as (
        select *
        from {{ ref('stg_sap__creditcard') }}
    )

    , stg_salesorderheader  as (
        select *
        from {{ ref('stg_sap__salesorderheader') }}
    )

  , join_tabelas as (
        select 
            stg_creditcard.id_cartao
            , stg_creditcard.nome_bandeira_cartao
    

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

    from stg_creditcard
       -- left join stg_creditcard on
        --    stg_salesorderheader.id_cartao = stg_creditcard.id_cartao 

       )

    , criar_chave as (
        select
            row_number() over (order by id_cartao) as pk_pagamento
            , *
        from join_tabelas
    )

    select *
    from criar_chave
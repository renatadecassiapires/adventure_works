-- ---------------------------------------- FATO ----------------------------
with
    dim_produtos as (
        select *
        from {{ ref('dim_produtos') }}
    ),
    dim_pagamento as (
        select *
        from {{ ref('dim_pagamento') }}
    ),
    dim_local as (
        select *
        from {{ ref('dim_local') }}
    ),
    dim_data as (
        select *
        from {{ ref('dim_data') }}
    ),
    dim_status as (
        select 
            id_pedido,
            id_status
        from {{ ref('stg_sap__salesorderheader') }}
    ),
    pedidos_itens as (
        with 
            stg_salesorderheader as (
                select *
                from {{ ref('stg_sap__salesorderheader') }}
            ),
            stg_salesorderdetail as (
                select *
                from {{ ref('stg_sap__salesorderdetail') }}
            ),
            join_tabelas as (
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
                    , stg_salesorderdetail.id_detalhe_pedido
                    , stg_salesorderdetail.quantidade_pedido
                    , stg_salesorderdetail.id_produto
                    , stg_salesorderdetail.id_oferta
                    , stg_salesorderdetail.preco_unitario
                    , stg_salesorderdetail.desconto_preco_unitario
                from stg_salesorderdetail
                left join stg_salesorderheader on 
                    stg_salesorderdetail.id_pedido = stg_salesorderheader.id_pedido
            ),
            criar_chave as (
                select 
                   cast(id_pedido as string) || cast(id_produto as string) as sk_pedido_item
                    , *
                from join_tabelas
            )
        select distinct *
        from criar_chave
    ),
    join_tabelas as (
        select
            dim_produtos.pk_produto as produto_fk,
            dim_pagamento.pk_pagamento as pagamento_fk,
            dim_local.pk_local as local_fk,
            pedidos_itens.sk_pedido_item,
            pedidos_itens.id_pedido,
            pedidos_itens.data_pedido,
            pedidos_itens.endereco_destino,
            pedidos_itens.subtotal_compra,
            pedidos_itens.total_compra,
            pedidos_itens.quantidade_pedido,
            pedidos_itens.preco_unitario,
            pedidos_itens.desconto_preco_unitario,
            dim_data.pk_data as data_fk,
            dim_status.id_status,
            case 
                when dim_status.id_status = 5 then "compra_aprovada"
                else "compra_nao_aprovada"
            end as status_compra,
            pedidos_itens.id_cliente as fk_customer -- Adicionando a chave estrangeira do cliente
        from pedidos_itens
        left join dim_produtos 
            on pedidos_itens.id_produto = dim_produtos.id_produto
        left join dim_pagamento
            on pedidos_itens.id_cartao = dim_pagamento.id_cartao
        left join dim_local 
            on pedidos_itens.endereco_destino = dim_local.id_endereco
        left join dim_data 
            on pedidos_itens.data_pedido = dim_data.date_actual
        left join dim_status
            on dim_status.id_pedido = pedidos_itens.id_pedido
    ),
    transformacoes as (
        select
            *,
            preco_unitario * quantidade_pedido as bruto,
            (1 - desconto_preco_unitario) * preco_unitario * quantidade_pedido as liquido,
            case
                when desconto_preco_unitario > 0 then true
                when desconto_preco_unitario = 0 then false
                else false
            end as is_desconto
        from join_tabelas
    )

select *
from transformacoes

-- arquivo: tabela_modelagem_desafio.sql

with dim_produtos as (
    select *
    from {{ ref('dim_produtos') }}
),
dim_b2c as (
    select *
    from {{ ref('dim_b2c') }}
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
    select *
    from {{ ref('int_vendas__pedidos_itens') }}
),
join_tabelas as (
    select
        dim_produtos.pk_produto as produto_fk,
        dim_b2c.pk_b2c as b2c_fk,
        dim_pagamento.pk_pagamento as pagamento_fk,
        dim_local.pk_local as local_fk,
        pedidos.sk_pedido_item,
        pedidos.id_pedido,
        pedidos.data_pedido,
        pedidos.endereco_destino,
        pedidos.subtotal_compra,
        pedidos.total_compra,
        pedidos.quantidade_pedido,
        pedidos.preco_unitario,
        pedidos.desconto_preco_unitario,
        dim_data.pk_data as data_fk,
        dim_status.id_status,
        case 
            when dim_status.id_status = 5 then 'compra_aprovada'
            else 'compra_nao_aprovada'
        end as status_compra
    from pedidos_itens as pedidos
    left join dim_produtos on pedidos.id_produto = dim_produtos.id_produto
    left join dim_b2c on pedidos.id_cliente = dim_b2c.id_cliente
    left join dim_pagamento on pedidos.id_cartao = dim_pagamento.id_cartao
    left join dim_local on pedidos.endereco_destino = dim_local.id_endereco
    left join dim_data on pedidos.data_pedido = dim_data.date_actual
    left join dim_status on dim_status.id_pedido = pedidos.id_pedido
),
Transformacoes as (
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

-- Criando a tabela final

{{ 
    config(
        materialized='table',
        unique_key='pk_produto'
    ) 
}}

select *
from Transformacoes

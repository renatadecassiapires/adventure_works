
with
    dim_produtos as (
    select *
    from {{ ref('dim_produtos') }}
)

, dim_b2c as (
    select *
    from {{ ref('dim_b2c') }}
)

,  dim_pagamento as (
    select *
    from {{ ref('dim_pagamento') }}
)

, dim_local as (
    select *
    from {{ ref('dim_local') }}
)

, dim_data as (
    select *
    from {{ ref('dim_data') }}
)
 
, dim_status as (
    select 
        id_pedido
        , id_status
    from {{ ref('stg_sap__salesorderheader') }}
)


,  pedidos_itens as (
    select *
    from {{ ref('int_vendas__pedidos_itens') }}

)

, join_tabelas as (
    select

    dim_produtos.pk_produto as produto_fk
   --, dim_produtos.id_produto
   --, dim_produtos.nome_produto
   --, dim_produtos.cor_produto
   --, dim_produtos.id_subcategoria
   --, dim_produtos.id_categoria
   --, dim_produtos.nome_subcategoria
   --, dim_produtos.nome_categoria

   , dim_b2c.pk_b2c as b2c_fk
   --, dim_b2c.id_representante
   --, dim_b2c.nome_loja
   --, dim_b2c.id_vendedor
   --, dim_b2c.id_cliente
   --, dim_b2c.id_pessoa
   --, dim_b2c.id_territorio
   --, dim_b2c.nome
   --, dim_b2c.sobrenome

   
    , dim_pagamento.pk_pagamento as pagamento_fk
   -- , dim_pagamento.nome_bandeira_cartao
   -- --, dim_pagamento.id_pedido
   -- , dim_pagamento.data_pedido
   -- , dim_pagamento.id_status
   -- , dim_pagamento.id_cliente
   -- , dim_pagamento.id_vendedor
   -- , dim_pagamento.id_territorio
   -- , dim_pagamento.id_cartao
   -- , dim_pagamento.id_compra_aprovada
   -- , dim_pagamento.subtotal_compra
   -- , dim_pagamento.total_compra

    , dim_local.pk_local as local_fk
    -- , dim_local.id_endereco
    -- , dim_local.nome_cidade
    -- , dim_local.id_estado
    -- , dim_local.nome_uf
    -- , dim_local.nome_sigla_pais
    -- , dim_local.nome_estado
    -- , dim_local.id_territorio
    -- , dim_local.nome_pais

    , pedidos.sk_pedido_item
    , pedidos.id_pedido
    , pedidos.data_pedido
   -- , pedidos.id_status
   -- , pedidos.id_cliente
   -- , pedidos.id_vendedor
   -- , pedidos.id_territorio
    , pedidos.endereco_destino
   -- , pedidos.id_cartao
   -- , pedidos.id_compra_aprovada
    , pedidos.subtotal_compra
    , pedidos.total_compra
   -- , pedidos.id_detalhe_pedido
    , pedidos.quantidade_pedido
    --, pedidos.id_produto
   -- , pedidos.id_oferta
    , pedidos.preco_unitario
    , pedidos.desconto_preco_unitario


     , dim_data.pk_data as data_fk
   --  , dim_data.date_actual
   --  , dim_data.day_of_month
    --, dim_data.year
   -- , dim_data.month_actual

    , dim_status.id_status
    , case 
        when dim_status.id_status = 5
        then "compra_aprovada"
        else "compra_nao_aprovada"
    end as status_compra

    from pedidos_itens as pedidos
    left join dim_produtos 
        on pedidos.id_produto = dim_produtos.id_produto
    left join dim_b2c 
        on pedidos.id_cliente = dim_b2c.id_cliente
        left join dim_pagamento
        on pedidos.id_cartao = dim_pagamento.id_cartao
    left join dim_local 
        on pedidos.endereco_destino = dim_local.id_endereco
    left join dim_data 
        on pedidos.data_pedido = dim_data.date_actual
    left join dim_status
        on dim_status.id_pedido = pedidos.id_pedido
     )


  , Transformacoes as (
        select
            *
            , preco_unitario * quantidade_pedido as bruto
            , (1 - desconto_preco_unitario) * preco_unitario * quantidade_pedido as liquido
            , case
                when desconto_preco_unitario > 0 then true
                when desconto_preco_unitario = 0 then false
                else false
            end as is_desconto
    from join_tabelas
     )

select *

from Transformacoes


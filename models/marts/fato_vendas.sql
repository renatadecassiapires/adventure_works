-- fato_vendas.sql

with pedidos_itens as (
    select *
    from {{ ref('int_vendas__pedidos_itens') }}
)

, dim_produtos as (
    select *
    from {{ ref('dim_produtos') }}
)

, dim_pagamento as (
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
 
, join_tabelas as (
    select
        pi.*,
        dp.pk_produto as produto_fk,
        dpag.pk_pagamento as pagamento_fk,
        dl.pk_local as local_fk,
        dd.pk_data as data_fk,
        pi.id_status, -- Aqui supomos que o status está em pedidos_itens
        case 
            when pi.id_status = 5 then 'compra_aprovada'
            else 'compra_nao_aprovada'
        end as status_compra
    from pedidos_itens pi
    left join dim_produtos dp on pi.id_produto = dp.id_produto
    left join dim_pagamento dpag on pi.id_pedido = dpag.id_pedido
    left join dim_local dl on pi.id_endereco_entrega = dl.id_endereco
    left join dim_data dd on pi.data_pedido = dd.date_actual
)

select
    produto_fk,
    pagamento_fk,
    local_fk,
    data_fk,
    id_status,
    status_compra,
    quantidade,
    valor_total
from join_tabelas

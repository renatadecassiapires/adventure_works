with 
    fato_vendas as (
        select 
            cast(id_pedido as string) as id_pedido
            , data_fk
        from {{ ref('fato_vendas') }}
    )
 
    , salesorderheadersalesreason as (
        select 
            cast(id_pedido as string) as id_pedido
            , cast(id_motivo as string) as id_motivo
        from {{ ref('stg_sap__salesorderheadersalesreason') }}
    )

     , salesreason as (
        select 
            cast(id_motivo as string) as id_motivo
            , nome_razao_motivo
            , nome_motivo
        from {{ ref('stg_sap__salesreason') }}
    )
 
    , join_sales_reason as (
        select
            salesorderheadersalesreason.id_pedido
            , salesorderheadersalesreason.id_motivo
            , salesreason.nome_razao_motivo
            , salesreason.nome_motivo
         from salesorderheadersalesreason

         left join salesreason on
            salesorderheadersalesreason.id_motivo = salesreason.id_motivo

    )
    
    , final as (
        select 
            fato_vendas.id_pedido
            , coalesce (join_sales_reason.id_motivo, "Nao_informado") as id_motivo
            , coalesce (join_sales_reason.nome_razao_motivo, "Nao_informado") as nome_razao_motivo
            , coalesce (join_sales_reason.nome_motivo, "Nao_informado") as nome_motivo
            , fato_vendas.data_fk
        from fato_vendas
        left join join_sales_reason on
            join_sales_reason.id_pedido = fato_vendas.id_pedido
    )
select *
from final
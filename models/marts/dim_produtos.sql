with 
    stg_sap__product as (
        select *
        from {{ ref('stg_sap__product') }}
    )
     , stg_sap__subcategory as (
        select *
        from {{ ref('stg_sap__subcategory') }}
    )

    , stg_category as (
        select *
        from {{ ref('stg_sap__category') }}
    )

     , join_tabelas as (
        select 
        stg_sap__product.id_produto
    ,   stg_sap__product.nome_produto
    ,   stg_sap__product.cor_produto
    ,   stg_sap__product.id_subcategoria

    --,   stg_sap__subcategory.id_subcategoria
    ,   stg_sap__subcategory.id_categoria
    ,   stg_sap__subcategory.nome_subcategoria


   -- ,   stg_sap__category.id_categoria
    ,   stg_sap__category.nome_categoria
    
     
      
from stg_sap__product
        left join stg_sap__subcategory on stg_sap__product.id_subcategoria= stg_sap__subcategory.id_subcategoria
        left join stg_sap__category on  stg_sap__subcategory.id_categoria = stg_sap__category.id_categoria
    )

    , criar_chave as (
        select
            row_number() over (order by id_produto) as pk_produto
            , *
        from join_tabelas
    )

    select *
    from criar_chave
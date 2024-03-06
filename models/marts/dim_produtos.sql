with 
    stg_product as (
        select *
        from {{ ref('stg_sap__product') }}
    )
     , stg_subcategory as (
        select *
        from {{ ref('stg_sap__subcategory') }}
    )

    , stg_category as (
        select *
        from {{ ref('stg_sap__category') }}
    )

     , join_tabelas as (
        select 
        stg_product.id_produto
    ,   stg_product.nome_produto
    ,   stg_product.cor_produto
    ,   stg_product.id_subcategoria

    --,   stg_subcategory.id_subcategoria
    ,   stg_subcategory.id_categoria
    ,   stg_subcategory.nome_subcategoria


   -- ,   stg_category.id_categoria
    ,   stg_category.nome_categoria
    
     
      
from stg_product
        left join stg_subcategory on stg_product.id_subcategoria= stg_subcategory.id_subcategoria
        left join stg_category on  stg_subcategory.id_categoria = stg_category.id_categoria
    )

    , criar_chave as (
        select
            row_number() over (order by id_produto) as pk_produto
            , *
        from join_tabelas
    )

    select *
    from criar_chave
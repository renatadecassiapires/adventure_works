with 
    stg_store as (
        select *
        from {{ ref('stg_sap__store') }}
    )
     , stg_customer as (
        select *
        from {{ ref('stg_sap__customer') }}
    )

    , stg_person as (
        select *
        from {{ ref('stg_sap__person') }}
    )

     , join_tabelas as (
        select 
        stg_store.id_representante
    ,   stg_store.nome_loja
    ,   stg_store.id_vendedor

    ,   stg_customer.id_cliente
    ,  stg_customer.id_pessoa
    --, stg_customer.id_loja
    ,   stg_customer .id_territorio


   --, stg_person.id_representante
    , stg_person.nome
    , stg_person.sobrenome

      
from stg_customer
        left join stg_person on
            stg_customer.id_pessoa= stg_person.id_representante
left join stg_store on
           stg_customer.id_loja= stg_store.id_representante
    )

    , criar_chave as (
        select
            row_number() over (order by id_cliente) as pk_b2c
            , *
        from join_tabelas
    )

select *
from criar_chave


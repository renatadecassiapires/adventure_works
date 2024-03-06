with 
    stg_endereco as (
        select *
        from {{ ref('stg_sap__address') }}
    )
     , stg_uf as (
        select *
        from {{ ref('stg_sap__stateprovince') }}
    )

    , stg_pais as (
        select *
        from {{ ref('stg_sap__countryregion') }}
    )

     , join_tabelas as (
        select 
        stg_endereco.id_endereco
        , stg_endereco.nome_cidade
        , stg_endereco.id_estado

       -- , stg_uf.id_estado
        , stg_uf.nome_uf
        , stg_uf.nome_sigla_pais
        , stg_uf.nome_estado
        , stg_uf.id_territorio
        
       -- , stg_pais.nome_sigla_pais
        , stg_pais.nome_pais

from stg_endereco
        left join stg_uf on
            stg_endereco. id_estado = stg_uf.id_estado
   

left join stg_pais on
            stg_pais.nome_sigla_pais =  stg_uf.nome_sigla_pais
    )

    , criar_chave as (
        select
            row_number() over (order by id_endereco) as pk_local
            , *
        from join_tabelas
    )

    select *
from criar_chave

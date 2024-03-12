with 
    stg_sap__address as (
        select 
            id_endereco,
            nome_cidade,
            id_estado
        from {{'stg_sap__address'}} 
    ),
    stg_sap__stateprovince as (
        select 
            id_estado,
            nome_uf,
            nome_sigla_pais,
            nome_estado,
            id_territorio
        from {{'stg_sap__stateprovince'}}
    ),
    stg_sap__countryregion as (
        select 
            nome_sigla_pais,
            nome_pais
        from {{'stg_sap__countryregion'}}
    ),
    join_tabelas as (
        select 
            se.id_endereco,
            se.nome_cidade,
            se.id_estado,
            su.nome_uf,
            su.nome_sigla_pais,
            su.nome_estado,
            su.id_territorio,
            sp.nome_pais
        from stg_sap__address se
        left join stg_sap__stateprovince su on se.id_estado = su.id_estado
        left join stg_sap__countryregion sp on su.nome_sigla_pais = sp.nome_sigla_pais
    ),
    criar_chave as (
        select
            row_number() over (order by id_endereco) as pk_local,
            jt.*
        from join_tabelas jt
    )

select *
from criar_chave

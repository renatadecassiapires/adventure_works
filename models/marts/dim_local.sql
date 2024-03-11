with 
    stg_endereco as (
        select 
            id_endereco,
            nome_cidade,
            id_estado
        from `adwsap.sap_adw.stg_sap__address`
    ),
    stg_uf as (
        select 
            id_estado,
            nome_uf,
            nome_sigla_pais,
            nome_estado,
            id_territorio
        from `adwsap.sap_adw.stg_sap__stateprovince`
    ),
    stg_pais as (
        select 
            nome_sigla_pais,
            nome_pais
        from `adwsap.sap_adw.stg_sap__countryregion`
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
        from stg_endereco se
        left join stg_uf su on se.id_estado = su.id_estado
        left join stg_pais sp on su.nome_sigla_pais = sp.nome_sigla_pais
    ),
    criar_chave as (
        select
            row_number() over (order by id_endereco) as pk_local,
            jt.*
        from join_tabelas jt
    )

select *
from criar_chave

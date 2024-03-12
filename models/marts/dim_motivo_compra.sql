with 
    stg_sap__salesreason as (
        select 
            id_motivo,
            nome_motivo,
            nome_razao_motivo
        from {{ ref('stg_sap__salesreason') }}
    )

select *
from stg_sap__salesreason






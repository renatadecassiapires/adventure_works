with 
    stg_salesreason as (
        select 
            {{ dbt_utils.generate_surrogate_key(['id_motivo']) }} as pk_motivo_compra
            , id_motivo
            , nome_motivo
            , nome_razao_motivo
        from {{ ref('stg_sap__salesreason') }}
    )

select *
from stg_salesreason


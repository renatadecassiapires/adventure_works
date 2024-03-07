with fonte_salesreason as (
    select 
        cast(salesreasonid as int) as id_motivo,
        cast(name as string) as nome_motivo,
        cast(reasontype as string) as nome_razao_motivo
    from {{ source('sap', 'salesreason') }}
)

select *
from fonte_salesreason

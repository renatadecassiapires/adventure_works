with
    fonte_store as (
        select 
        cast(businessentityid as int) as id_representante
        , cast(name as string) as nome_loja
        , cast(salespersonid as int) as id_vendedor
        -- , cast(demographics as string) as nome
        --, cast(rowguid as tipo) as nome
        --, cast(modifieddate as tipo) as nome
        from {{ source('sap', 'store') }}
    )

select *
from fonte_store

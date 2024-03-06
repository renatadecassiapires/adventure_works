with
    fonte_stateprovince as (
        select 
        cast(stateprovinceid as int) as id_estado
        , cast(stateprovincecode as string) as nome_uf
        , cast(countryregioncode as string) as nome_sigla_pais
       -- , cast(isonlystateprovinceflag as tipo) as nome
        , cast(name as string) as nome_estado
        , cast(territoryid as int) as id_territorio
       --, cast(rowguid as tipo) as nome
       --, cast(modifieddate as tipo) as nome
        from {{ source('sap', 'stateprovince') }}
     )

select *
from fonte_stateprovince

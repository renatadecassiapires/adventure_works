with
    fonte_countryregion as (
        select 
        cast(countryregioncode as string) as nome_sigla_pais
       , cast( name as string) as nome_pais
       --, cast( modifieddate as tipo) as nome
        from {{ source('sap', 'countryregion') }}
    )

select *
from fonte_countryregion


--with
--   fonte_address as (
--        select 
--       cast(addressid as int) as id_endereco
--       --, cast( addressline1 as tipo) as nome
--       --, cast( addressline2 as tipo) as nome
--       , cast( city as string) as nome_cidade
--       , cast( stateprovinceid as int) as id_estado
--       --, cast( postalcode as tipo) as nome
--       --, cast( spatiallocation as tipo) as nome
--       --, cast( rowguid as tipo) as nome
--       --, cast( modifieddate as tipo) as nome
--        from {{ source('sap', 'address') }}
--    )
--select *
--from fonte_address


with
    fonte_adress as (
        select *
        from {{ source('sap', 'address') }}
    )

select *
from fonte_address
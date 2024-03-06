with
    fonte_subcategory as (
        select 
        cast(productsubcategoryid as int) as id_subcategoria
       , cast(productcategoryid as int) as id_categoria
       , cast(name as string) as nome_subcategoria
       --, cast(rowguid as tipo) as nome
       --, cast(modifieddate as tipo) as nome
       , from {{ source('sap', 'productsubcategory') }}
    )
select *
from fonte_subcategory
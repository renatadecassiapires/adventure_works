with
    fonte_category as (
        select 
        cast (productcategoryid as int) as id_categoria
       , cast (name as string) as nome_categoria
        -- cast (rowguid as tipo) as nome
        --cast (modifieddate as tipo) as nome
        from {{ source('sap', 'productcategory') }}
    )
select *
from fonte_category

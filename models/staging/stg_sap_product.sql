with
    fonte_product as (
        select 
        cast(productid as int) as id_produto
        , cast(name as string) as nome_produto
        -- , cast(productnumber as int) as nome
        --, cast(makeflag as int) as nome
        --, cast(finishedgoodsflag as int) as nome
        , cast(color as string) as cor_produto
        -- , cast(safetystocklevel as int) as nome
        --, cast(reorderpoint as int) as nome
        --, cast(standardcost as int) as nome
        -- , cast(listprice as int) as nome
        -- , cast(size as int) as nome
        -- , cast(sizeunitmeasurecode as int) as nome
        -- , cast(weightunitmeasurecode as int) as nome
        -- , cast(weight as int) as nome
        -- , cast(daystomanufacture as int) as nome
        -- , cast(productline as int) as nome
        -- , cast(class as int) as nome
        -- , cast(style as int) as nome
        , cast(productsubcategoryid as int) as id_subcategoria
        -- , cast(productmodelid as int) as nome
        -- , cast(sellstartdate as int) as nome
        -- , cast(sellenddate as int) as nome
        -- , cast(discontinueddate as int) as nome
        -- , cast(rowguid as int) as nome
        -- , cast(modifieddate as int) as nome
        from {{ source('sap', 'product') }}
    )

select *
from fonte_product
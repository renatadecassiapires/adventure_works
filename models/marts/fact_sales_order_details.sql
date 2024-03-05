with
    vendedor as (
        select
            salesperson.businessentityid as salesperson_id,
            salesperson.territoryid as territoryid_salesperson,
            person.businessentityid,
            person.persontype,
            concat(
                coalesce(person.title, ''),
                coalesce(person.firstname, ''),
                coalesce(person.middlename, ''),
                coalesce(person.lastname, ''),
                coalesce(person.suffix, '')
            ) as salesperson_fullname,
            customer.storeid

        from dbt_rpires.salesperson as salesperson

        left join
            dbt_rpires.customer as customer
            on salesperson.businessentityid = customer.customerid
        left join
            dbt_rpires.person as person
            on salesperson.businessentityid = person.businessentityid
    ),

    loja as (
        select
            store.businessentityid as storeid,
            store.name as name_store,
            store.salespersonid,
            businessentityaddress.addressid,
            address.city as city_store,
            address.stateprovinceid,
            stateprovince.countryregioncode,
            stateprovince.isonlystateprovinceflag,
            stateprovince.name as stateprovince_name,
            stateprovince.territoryid as territoryid_store

        from dbt_rpires.store as store

        left join
            dbt_rpires.businessentityaddress as businessentityaddress
            on store.businessentityid = businessentityaddress.businessentityid

        left join
            dbt_rpires.address as address
            on businessentityaddress.addressid = address.addressid
        left join
            dbt_rpires.stateprovince as stateprovince
            on address.stateprovinceid = stateprovince.stateprovinceid
    ),

    produto as (
        select
            product.productid,
            product.name as product_name,
            product.standardcost,
            product.listprice,
            product.daystomanufacture,
            case
                when product.productline = 'R' then 'road'
                when product.productline = 'M' then 'mountain'
                when product.productline = 'T' then 'touring'
                when product.productline = 'S' then 'standard'
                else 'undefined'
            end as product_line,
            case
                when product.class = 'H' then 'high'
                when product.class = 'M' then 'medium'
                when product.class = 'L' then 'low'
                else 'undefined'
            end as product_class,
            case
                when product.style = 'W' then 'womens'
                when product.style = 'M' then 'mens'
                when product.style = 'U' then 'universal'
                else 'undefined'
            end as product_style,
            product.productmodelid,
            productmodel.name as productmodel_name

        from dbt_rpires.product as product

        left join
            dbt_rpires.productmodel as productmodel
            on product.productmodelid = productmodel.productmodelid
    )

select
    ifnull(concat('SO', cast(salesorderheader.salesorderid as string)), '* ERROR *') as salesordernumber,
    cast(substr(salesorderheader.orderdate, 1, 10) as date) as orderdate,
    substr(salesorderheader.orderdate, 1, 7) as orderdate_yearmonth,
    salesorderheader.onlineorderflag as onlineorderflag,
    salesorderdetail.orderqty,
    salesorderdetail.unitprice,
    produto.product_name,
    produto.productmodel_name,
    produto.standardcost,
    produto.listprice,
    produto.daystomanufacture,
    produto.product_line,
    produto.product_class,
    produto.product_style,
    vendedor.salesperson_fullname,
    vendedor.territoryid_salesperson,
    loja.name_store,
    loja.countryregioncode

from dbt_rpires.salesorderheader as salesorderheader

left join
    dbt_rpires.salesorderdetail as salesorderdetail
    on salesorderheader.salesorderid = salesorderdetail.salesorderid

left join vendedor on salesorderheader.salespersonid = vendedor.salesperson_id

left join loja on vendedor.storeid = loja.storeid

left join produto on salesorderdetail.productid = produto.productid

order by salesordernumber, orderdate

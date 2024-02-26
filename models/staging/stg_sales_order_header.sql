WITH source_data AS (
    SELECT
        salesorderid,
        shipmethodid,
        billtoaddressid,
        modifieddate,
        rowguid,
        taxamt,
        shiptoaddressid,
        onlineorderflag,
        territoryid,
        status AS order_status,
        orderdate,
        creditcardapprovalcode,
        subtotal,
        creditcardid,
        currencyrateid,
        revisionnumber,
        freight,
        duedate,
        totaldue,
        customerid,
        salespersonid,
        shipdate,
        accountnumber
    FROM `adventureworksdesafiolh.dbt_rpires.salesorderheader`
)
SELECT *
FROM source_data

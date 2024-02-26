WITH customers AS (
    SELECT
        customer_sk,
        customerid
    FROM {{ ref('dim_customers') }}
),

creditcards AS (
    SELECT
        creditcard_sk,
        creditcardid
    FROM {{ ref('dim_creditcards') }}
),

locations AS (
    SELECT
        shiptoaddress_sk,
        shiptoaddressid
    FROM {{ ref('dim_locations') }}
),

reasons AS (
    SELECT
        salesorderid,
        reason_name_aggregated
    FROM {{ ref('dim_reasons') }}
),

products AS (
    SELECT
        product_sk,
        productid
    FROM {{ ref('dim_products') }}
),

salesorderdetail AS (
    SELECT
        d.salesorderid,
        p.product_sk AS product_fk,
        d.orderqty,
        d.unitprice,
        d.unitprice * d.orderqty AS revenue_wo_taxandfreight,
        IFNULL(r.reason_name_aggregated, 'Not indicated') AS reason_name_final
    FROM {{ ref('stg_salesorderdetail') }} d
    LEFT JOIN products p ON d.productid = p.productid
    LEFT JOIN reasons r ON d.salesorderid = r.salesorderid
),

salesorderheader AS (
    SELECT
        h.salesorderid,
        c.customer_sk AS customer_fk,
        cc.creditcard_sk AS creditcard_fk,
        l.shiptoaddress_sk AS shiptoadress_fk,
        CASE 
            WHEN h.order_status = 1 THEN 'In_process'
            WHEN h.order_status = 2 THEN 'Approved'
            WHEN h.order_status = 3 THEN 'Backordered' 
            WHEN h.order_status = 4 THEN 'Rejected' 
            WHEN h.order_status = 5 THEN 'Shipped'
            WHEN h.order_status = 6 THEN 'Cancelled' 
            ELSE 'no_status'
        END AS order_status_name,
        h.orderdate
    FROM {{ ref('stg_salesorderheader') }} h
    LEFT JOIN customers c ON h.customerid = c.customerid
    LEFT JOIN creditcards cc ON h.creditcardid = cc.creditcardid
    LEFT JOIN locations l ON h.shiptoaddressid = l.shiptoaddressid
)

SELECT
    d.salesorderid,
    d.product_fk,
    h.customer_fk,
    h.shiptoadress_fk,
    h.creditcard_fk,
    d.unitprice,
    d.orderqty,
    d.revenue_wo_taxandfreight,
    d.reason_name_final,
    h.orderdate,
    h.order_status_name
FROM salesorderdetail d
JOIN salesorderheader h ON d.salesorderid = h.salesorderid;

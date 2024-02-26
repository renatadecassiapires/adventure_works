WITH stg_salesorderheader AS (
    SELECT *
    FROM {{ ref('stg_salesorderheader') }}
),

stg_salesorderdetail AS (
    SELECT DISTINCT
        productid
    FROM {{ ref('stg_salesorderdetail') }}
    WHERE productid IS NOT NULL
),

stg_product AS (
    SELECT *
    FROM {{ ref('stg_product') }}
),

transformed AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY s.productid) AS product_sk, -- Chave substituta autoincremental
        s.productid,
        p.product_name
    FROM stg_salesorderdetail s
    LEFT JOIN stg_product p ON s.productid = p.productid
)

SELECT *
FROM transformed
WITH stg_salesorderheadersalesreason AS (
    SELECT *
    FROM {{ ref('stg_salesorderheadersalesreason') }}
),

stg_salesreason AS (
    SELECT *
    FROM {{ ref('stg_salesreason') }}
),

reasonbyorderid AS (
    SELECT
        s.salesorderid,
        r.reason_name
    FROM stg_salesorderheadersalesreason s
    LEFT JOIN stg_salesreason r ON s.salesreasonid = r.salesreasonid
),

transformed AS (
    SELECT
        salesorderid,
        STRING_AGG(reason_name, ', ') AS reason_name_aggregated
    FROM reasonbyorderid
    GROUP BY salesorderid
)

SELECT *
FROM transformed;

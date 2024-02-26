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
        so.salesorderid,
        sr.reason_name
    FROM stg_salesorderheadersalesreason so
    LEFT JOIN stg_salesreason sr ON so.salesreasonid = sr.salesreasonid 
),

transformed AS (
    SELECT
        salesorderid,
        -- Utilize STRING_AGG para BigQuery ou a função equivalente no seu banco de dados para agrupar os nomes das razões
        STRING_AGG(reason_name, ', ') AS reason_name_aggregated
    FROM reasonbyorderid
    GROUP BY salesorderid
)

SELECT *
FROM transformed

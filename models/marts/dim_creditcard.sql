WITH stg_salesorderheader AS (
    SELECT DISTINCT
        creditcardid
    FROM {{ ref('stg_salesorderheader') }}
    WHERE creditcardid IS NOT NULL
),

stg_creditcard AS (
    SELECT *
    FROM {{ ref('stg_creditcard') }}
),

transformed AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY s.creditcardid) AS creditcard_sk, -- auto-incremental surrogate key
        s.creditcardid,
        c.cardtype
    FROM stg_salesorderheader s
    LEFT JOIN stg_creditcard c ON s.creditcardid = c.creditcardid
)

SELECT *
FROM transformed;

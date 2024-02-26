CREATE TABLE dim_creditcard AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY s.creditcardid) AS creditcard_sk,
        s.creditcardid,
        c.cardtype,
        c.cardnumber,
        c.expmonth,
        c.expyear,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', c.modifieddate) AS modifieddate
    FROM stg_salesorderheader s
    LEFT JOIN stg_creditcard c ON s.creditcardid = c.creditcardid
    WHERE s.creditcardid IS NOT NULL
);

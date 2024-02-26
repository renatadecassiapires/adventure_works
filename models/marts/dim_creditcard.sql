-- models/marts/dim_creditcard.sql

SELECT 
    ROW_NUMBER() OVER () AS creditcard_sk,
    s.creditcardid,
    c.cardtype,
    c.cardnumber,
    c.expmonth,
    c.expyear,
    TIMESTAMP(c.modifieddate) AS modifieddate
FROM 
    `adventureworksdesafiolh.dbt_rpires.stg_salesorderheader` s
LEFT JOIN 
    `adventureworksdesafiolh.dbt_rpires.stg_creditcard` c ON s.creditcardid = c.creditcardid
WHERE 
    s.creditcardid IS NOT NULL;

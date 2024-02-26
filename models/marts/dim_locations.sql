WITH stg_salesorderheader AS (
    SELECT DISTINCT
        shiptoaddressid
    FROM {{ ref('stg_salesorderheader') }}
    WHERE shiptoaddressid IS NOT NULL
),

stg_address AS (
    SELECT *
    FROM {{ ref('stg_address') }}
),

stg_stateprovince AS (
    SELECT *
    FROM {{ ref('stg_stateprovince') }}
),

stg_countryregion AS (
    SELECT *
    FROM {{ ref('stg_countryregion') }}
),

transformed AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY s.shiptoaddressid) AS location_sk, -- Chave substituta autoincremental
        s.shiptoaddressid,
        a.city AS city_name,
        sp.state_name,
        cr.country_name
    FROM stg_salesorderheader s
    LEFT JOIN stg_address a ON s.shiptoaddressid = a.addressid
    LEFT JOIN stg_stateprovince sp ON a.stateprovinceid = sp.stateprovinceid
    LEFT JOIN stg_countryregion cr ON sp.countryregioncode = cr.countryregioncode
)

SELECT *
FROM transformed;

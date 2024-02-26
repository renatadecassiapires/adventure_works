-- models/staging/stg_countryregion.sql

WITH source_data AS (
    SELECT
        countryregioncode,
        modifieddate,
        name AS country_name
    FROM `adventureworksdesafiolh.dbt_rpires.countryregion`
)
SELECT *
FROM source_data;

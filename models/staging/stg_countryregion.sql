CREATE OR REPLACE VIEW `adventureworksdesafiolh.dbt_rpires.stg_countryregion` AS
WITH source_data AS (
    SELECT
        countryregioncode,
        modifieddate,
        name AS country_name
    FROM `adventureworksdesafiolh.dbt_rpires.countryregion`
)
SELECT *
FROM source_data;

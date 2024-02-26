WITH source_data AS (
    SELECT
        businessentityid,
        name AS store_name,
        salespersonid,
        modifieddate
    FROM `adventureworksdesafiolh.dbt_rpires.store`
)
SELECT *
FROM source_data

WITH source_data AS (
    SELECT
        salesorderid,
        modifieddate,
        salesreasonid
    FROM `adventureworksdesafiolh.dbt_rpires.salesorderheadersalesreason`
)
SELECT *
FROM source_data

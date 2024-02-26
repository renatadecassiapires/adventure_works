WITH source_data AS (
    SELECT
        salesreasonid,
        name AS reason_name,
        reasontype,
        modifieddate
    FROM `adventureworksdesafiolh.dbt_rpires.salesreason`
)
SELECT *
FROM source_data

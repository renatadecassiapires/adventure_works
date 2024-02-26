WITH source_data AS (
    SELECT
        stateprovinceid,
        countryregioncode,
        modifieddate,
        rowguid,
        name AS state_name,
        territoryid,
        isonlystateprovinceflag,
        stateprovincecode
    FROM `adventureworksdesafiolh.dbt_rpires.stateprovince`
)
SELECT *
FROM source_data

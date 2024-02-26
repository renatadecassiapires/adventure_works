WITH source_data AS (
    SELECT
        businessentityid,
        title,
        firstname,
        middlename,
        lastname,
        persontype,
        namestyle,
        suffix,
        modifieddate,
        rowguid,
        emailpromotion
    FROM `adventureworksdesafiolh.dbt_rpires.person` -- Replace with your actual project and dataset name
)
SELECT *
FROM source_data

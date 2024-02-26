-- models/marts/dim_customers.sql

CREATE OR REPLACE VIEW `adventureworksdesafiolh`.`dbt_rpires`.`dim_customers` AS
WITH stg_customer AS (
    SELECT 
        customerid,
        personid,
        storeid
    FROM `adventureworksdesafiolh.dbt_rpires.stg_customer`
),
stg_person AS (
    SELECT
        businessentityid,
        CONCAT(IFNULL(firstname, ''), ' ', IFNULL(middlename, ''), ' ', IFNULL(lastname, '')) AS fullname
    FROM `adventureworksdesafiolh.dbt_rpires.stg_person`
),
stg_store AS (
    SELECT
        businessentityid AS storebusinessentityid,
        store_name
    FROM `adventureworksdesafiolh.dbt_rpires.stg_store`
),
transformed AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY stg_customer.customerid) AS customer_sk,
        stg_customer.customerid,
        stg_person.businessentityid,
        stg_person.fullname,
        stg_store.storebusinessentityid,
        stg_store.store_name,
        CASE
            WHEN stg_person.fullname IS NULL THEN stg_store.store_name
            ELSE stg_person.fullname 
        END AS customer_fullname
    FROM stg_customer
    LEFT JOIN stg_person ON stg_customer.personid = stg_person.businessentityid
    LEFT JOIN stg_store ON stg_customer.storeid = stg_store.storebusinessentityid
)
SELECT *
FROM transformed;

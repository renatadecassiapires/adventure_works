with 

department as (

    select * from {{ source('sap_adw', 'department') }}

)

select * from department

with stg_customer as (
    select 
        id_cliente,
        id_pessoa,
        id_loja,
        id_territorio
    from `adwsap.sap_adw.stg_sap__customer`
),

stg_person as (
    select
        id_representante,
        nome,
        sobrenome
    from `adwsap.sap_adw.stg_sap__person`
),

stg_store as (
    select
        id_representante,
        nome_loja,
        id_vendedor
    from `adwsap.sap_adw.stg_sap__store`
),

transformed as (
    select
        row_number() over (order by stg_customer.id_cliente) as customer_sk,
        stg_customer.id_cliente,
        stg_person.id_representante as person_id,
        concat(stg_person.nome, ' ', stg_person.sobrenome) as person_name,
        stg_store.id_representante as store_id,
        stg_store.nome_loja as store_name,
        stg_store.id_vendedor as seller_id
    from stg_customer
    left join stg_person on stg_customer.id_pessoa = stg_person.id_representante
    left join stg_store on stg_customer.id_loja = stg_store.id_representante
)

select *
from transformed

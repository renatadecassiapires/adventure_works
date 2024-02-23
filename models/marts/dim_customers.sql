with source as (
    select
        c.customerid as source_customer_id,
        p.firstname,
        p.lastname,
        p.middlename,
        -- concatenação dos nomes com manejo de nulos
        coalesce(p.firstname, '') || ' ' ||
        coalesce(p.middlename, '') || ' ' ||
        coalesce(p.lastname, '') as fullname
    from {{ ref('stg_customer') }} c
    left join {{ ref('stg_person') }} p
    on c.personid = p.businessentityid
)

select
    row_number() over () as customer_sk, -- chave surrogate
    source_customer_id,
    fullname
from source


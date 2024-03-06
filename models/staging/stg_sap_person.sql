with
    fonte_person as (
        select 
        cast(businessentityid as int) as id_representante
        -- , cast(  persontypea as tipo) as nome
        -- , cast(  namestylea as tipo) as nome
        -- , cast(  titlea as tipo) as nome
        , cast(  firstname as string) as nome
        -- , cast(  middlenamea as tipo) as nome
        , cast(  lastname as string) as sobrenome
        -- , cast(  suffixa as tipo) as nome
        -- , cast(  emailpromotiona as tipo) as nome
        -- , cast(  additionalcontactinfoa as tipo) as nome
        -- , cast(  demographicsa as tipo) as nome
        -- , cast(  rowguida as tipo) as nome
        -- , cast(  modifieddatea as tipo) as nome
        from {{ source('sap', 'person') }}
    )
select *
from fonte_person



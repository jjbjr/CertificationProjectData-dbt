with
    fonte_pessoas as (
        select *                
        from {{ source('sap', 'person') }}
    )

    , renomear as (
        select
            cast(businessentityid as integer) as fk_id_entidadenegocio 						
            , cast(persontype as string) as tipo_pessoa						
            --, namestyle						
            --, title						
            , cast((firstname|| ' ' || lastname) as string) as cliente					
            --, middlename
            --, suffix						
            --, emailpromotion						
            --, additionalcontactinfo						
            --, demographics						
            , cast(rowguid as string) as rowguid_pessoa					
            , cast(modifieddate as datetime) as data_modificada_pessoa			               
        from fonte_pessoas
    )
select *
from renomear
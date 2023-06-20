with
    fonte_entidadenegocios as (
        select *                
        from {{ source('sap', 'businessentity') }}
    )
    , renomear as (
        select
            cast (businessentityid as integer) as pk_id_entidadenegocio				
            , cast (rowguid as string) as rowguid_entidadenegocio				
            , cast (modifieddate as datetime) as data_modificada_entidadenegocio           
        from fonte_entidadenegocios
    )
select *
from renomear

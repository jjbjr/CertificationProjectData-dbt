with
    fonte_enderecoentidades as (
        select *                
        from {{ source('sap', 'businessentityaddress') }}
    )
    , renomear as (
        select
            cast(businessentityid as integer) as fk_id_entidadenegocio				
            , cast(addressid as integer) as fk_id_endereco					
            --, addresstypeid					
            , cast(rowguid as string) as rowguid_enderecoentidades				
            , cast(modifieddate as datetime) as data_modificada_enderecoentidades      
        from fonte_enderecoentidades
    )
select *
from renomear

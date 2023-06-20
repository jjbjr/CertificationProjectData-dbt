with
    fonte_endereco as (
        select *                
        from {{ source('sap', 'address') }}
    )
    , renomear as (
        select
            cast(addressid as integer) as pk_id_endereco				
            --, addressline1					
            --, addressline2					
            , cast(city as string) as cidade					
            , cast(stateprovinceid as integer) as fk_id_estado 					
            --, postalcode					
            --, spatiallocation					
            , cast(rowguid as string) as rowguid_endereco					
            , cast(modifieddate as datetime) as data_modificada_endereco           
        from fonte_endereco
    )
select *
from renomear

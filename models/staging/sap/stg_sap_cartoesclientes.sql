with
    fonte_cartoesclientes as (
        select *                
        from {{ source('sap', 'personcreditcard') }}
    )
    , renomear as (
        select
            cast(businessentityid as integer) as fk_id_entidadenegocio					
            , cast(creditcardid as integer) as fk_id_cartao					
            , cast(modifieddate as datetime) as data_modificada_cartoesclientes		       
        from fonte_cartoesclientes
    )
select *
from renomear
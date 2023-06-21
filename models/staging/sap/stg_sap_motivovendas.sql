/* Tabela salesreason */


with
    fonte_motivovendas as (
        select *                
        from {{ source('sap', 'salesreason') }}
    )
    , renomear as (
        select
            cast(salesreasonid as integer) as pk_id_motivovenda					
            , cast(name	as string) as motivo				
            --, reasontype					
            , cast(modifieddate as datetime) as data_modificada_motivovenda       
        from fonte_motivovendas
    )
select *
from renomear
/* Tabela salesorderheadersalesreason */

with
    fonte_pedidomotivos as (
        select *                
        from {{ source('sap', 'salesorderheadersalesreason') }}
    )
    , renomear as (
        select
            cast(salesorderid as integer) as fk_id_pedido				
            , cast(salesreasonid as integer) as fk_id_motivovenda			
            , cast(modifieddate as datetime) as data_modificada_crosspedidomotivo          
        from fonte_pedidomotivos
    )
select *
from renomear

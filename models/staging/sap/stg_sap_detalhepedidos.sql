/* Tabela salesorderdetail*/

with
    fonte_detalhespedidos as (
        select *                
        from {{ source('sap', 'salesorderdetail') }}
    )
    , renomear as (
        select
            cast(salesorderid as integer) as fk_id_pedido				
            , cast(salesorderdetailid as integer) as pk_id_detalhepedido					
            --, carriertrackingnumber 					
            , cast(orderqty	as integer) as quantidade				
            , cast(productid as integer) as fk_id_produto					
            --, specialofferid					
            , cast(unitprice as decimal) as precounitario					
            , cast(unitpricediscount as decimal) as desconto					
            , cast(rowguid as string) as rowguid_detalhespedido					
            , cast(modifieddate as datetime) as data_modificada_detalhespedido       
        from fonte_detalhespedidos
    )
select *
from renomear

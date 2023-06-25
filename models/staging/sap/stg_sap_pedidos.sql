/* Table salesorderheader*/

with
    fonte_pedidos as (
        select *                
        from {{ source('sap', 'salesorderheader') }}
    )
    , renomear as (
        select
            cast(salesorderid as integer) as pk_id_pedido				
            , cast(revisionnumber as integer) as revisao_pedido					
            , cast(orderdate as datetime) as data_pedido					
            --, duedate					
            , cast(shipdate as datetime) as data_envio					
            , cast(status as integer) as status_pedido					
            --, onlineorderflag					
            , cast(purchaseordernumber as string) as ordem_compra					
            , cast(accountnumber as string) as numero_conta_financeiro					
            , cast(customerid as integer) as fk_id_cliente				
            , cast(salespersonid as integer) as fk_id_entidadenegocio					
            , cast(territoryid as integer) as fk_id_territorio					
            --, billtoaddressid					
            , cast(shiptoaddressid as integer) as fk_id_endereco					
            --, shipmethodid					
            , cast(creditcardid as integer) as fk_id_cartao					
            --, creditcardapprovalcode					
            --, currencyrateid					
            , cast(subtotal as decimal) as subtotal_pedido					
            --, taxamt					
            --, freight					
            --, totaldue					
            --, comment					
            , cast(rowguid as string) as rowguid_pedido				
            , cast(modifieddate as datetime) as data_modificada_pedido		               
        from fonte_pedidos
    )
select *
from renomear
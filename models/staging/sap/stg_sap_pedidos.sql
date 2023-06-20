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
            , cast(customerid) as fk_id_cliente				
            , cast(salespersonid) as fk_id_vendedor					
            , cast(territoryid) as fk_id_territorio					
            --, billtoaddressid					
            , cast(shiptoaddressid					
            , cast(shipmethodid					
            , cast(creditcardid					
            , cast(creditcardapprovalcode					
            , cast(currencyrateid					
            , cast(subtotal					
            , cast(taxamt					
            , cast(freight					
            , cast(totaldue					
            , cast(comment					
            , cast(rowguid					
            , cast(modifieddate		               
        from fonte_pedidos
    )
select *
from renomear
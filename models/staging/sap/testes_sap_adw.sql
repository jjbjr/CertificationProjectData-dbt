with
    src_soh as (
        select *                
        from {{ source('sap', 'salesorderheader') }}
    )

    , src_sod as (
        select *
        from {{ source('sap', 'salesorderdetail') }}
    )

    , src_shr as(
        select *
        from {{ source('sap', 'salesorderheadersalesreason') }}
    )

    , src_sr as (
        select *
        from {{ source('sap', 'salesreason') }}
    )
select *
from src_sod
left join src_soh on src_sod.salesorderid = src_soh.salesorderid
left join src_shr on src_sod.salesorderid = src_shr.salesorderid
left join src_sr on src_shr.salesreasonid = src_sr.salesreasonid
where src_sod.salesorderid = 45393




-- sales order id = 45393
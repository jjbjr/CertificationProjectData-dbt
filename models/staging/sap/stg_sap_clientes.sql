with
    fonte_clientes as (
        select *            	
        from {{ source('sap', 'customer') }}
    )

    , renomear as (
        select
            cast(customerid	as integer) as pk_id_cliente			
            , cast(personid as integer) as fk_id_pessoa	
            , cast(storeid as integer) as fk_id_loja
            , cast(territoryid as integer) as fk_id_territorio			
            , cast(rowguid as string) as rowguid
            , cast(modifieddate as string) as data_modificada		
        from fonte_clientes
    )
select *
from renomear
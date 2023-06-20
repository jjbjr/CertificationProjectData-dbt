with
    fonte_modeloprodutos as (
        select *                
        from {{ source('sap', 'productmodel') }}
    )
    , renomear as (
        select
            cast(productmodelid as integer) as pk_id_modeloproduto					
            , cast(name as string) as modelo 						
            --, catalogdescription						
            --, instructions						
            , cast(rowguid as string) as rowguid_modeloproduto						
            , cast(modifieddate as datetime) as data_modificada_modeloproduto	            
        from fonte_modeloprodutos
    )
select *
from renomear
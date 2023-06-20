with
    fonte_categoriaprodutos as (
        select *                
        from {{ source('sap', 'productcategory') }}
    )
    , renomear as (
        select
            cast(productcategoryid as integer) as pk_id_categoria				
            , cast(name as string) as categoria				
            , cast (rowguid as string) as rowguid_categoria			
            , cast (modifieddate as datetime) as data_modificada_categoria           
        from fonte_categoriaprodutos
    )
select *
from renomear
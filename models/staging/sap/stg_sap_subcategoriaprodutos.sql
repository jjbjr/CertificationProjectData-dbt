with
    fonte_subcategoriaprodutos as (
        select *                
        from {{ source('sap', 'productsubcategory') }}
    )
    , renomear as (
        select
            cast(productsubcategoryid as integer) as pk_id_subcategoria				
            , cast(productcategoryid as integer) as fk_id_categoria					
            , cast(name as string) as subcategoria			
            , cast(rowguid as string) rowguid_subcategoria				
            , cast(modifieddate as datetime) data_modificada_subcategoria       
        from fonte_subcategoriaprodutos
    )
select *
from renomear
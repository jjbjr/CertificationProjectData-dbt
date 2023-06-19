with
    fonte_produtos as (
        select *                
        from {{ source('sap', 'product') }}
    )
    , renomear as (
        select
            cast (productid as integer) as pk_id_produto						
            , cast(name as string) as produto						
            , cast(productnumber as string) as numero_produto						
            --, makeflag						
            --, finishedgoodsflag						
            --, color						
            --, safetystocklevel						
            --, reorderpoint						
            , cast(standardcost	as decimal) as preco_custo					
            , cast(listprice as decimal) as preco_venda						
            --, size						
            --, sizeunitmeasurecode						
            --, weightunitmeasurecode						
            --, weight						
            --, daystomanufacture						
            --, productline						
            --, class						
            --, style						
            , cast(productsubcategoryid as integer) as fk_id_subcategoria						
            , cast(productmodelid as integer)  as fk_id_modelo						
            --, sellstartdate						
            --, sellenddate						
            --, discontinueddate						
            , cast(rowguid as string) as rowguid_produto
            , cast(modifieddate as datetime) as data_modificada_produto            
        from fonte_produtos
    )
select *
from renomear
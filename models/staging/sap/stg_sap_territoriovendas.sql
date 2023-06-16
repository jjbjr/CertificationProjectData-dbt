with
    fonte_territoriovendas as (
        select *                
        from {{ source('sap', 'salesterritory') }}
    )
    , renomear as (
        select
            cast(territoryid as integer) as pk_id_territorio				
            , cast(name as string) as territorio					
            , cast(countryregioncode as string) as sigla					
            , cast (`group` as string) as regiao					
            --, salesytd				
            --, saleslastyear				
            --, costytd				
            --, costlastyear				
            , cast (rowguid as string) as rowguid_territoriovendas					
            , cast (modifieddate as datetime) as data_modificada_territoriovendas		               
        from fonte_territoriovendas
    )
select *
from renomear
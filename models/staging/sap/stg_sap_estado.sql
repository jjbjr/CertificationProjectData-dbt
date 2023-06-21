with
    fonte_estado as (
        select *                
        from {{ source('sap', 'stateprovince') }}
    )
    , renomear as (
        select
            cast(stateprovinceid as integer) as pk_id_estado					
            , cast(stateprovincecode as string) as sigla_estado					
            , cast(countryregioncode as string) as sigla_pais					
            --, isonlystateprovinceflag				
            , cast(name as string) as estado					
            , cast(territoryid as integer)	as fk_id_territorio				
            , cast(rowguid as string) as rowguid_estado					
            , cast(modifieddate as datetime) as data_modificada_estado      
        from fonte_estado
    )
select *
from renomear

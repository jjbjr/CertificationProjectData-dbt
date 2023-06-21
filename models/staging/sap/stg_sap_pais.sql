with
    fonte_pais as (
        select *                
        from {{ source('sap', 'countryregion') }}
    )
    , renomear as (
        select
            cast(countryregioncode	as string) as sigla_pais			
            , cast(name	as string) as pais			
            , cast(modifieddate as datetime) as data_modificada_pais      
        from fonte_pais
    )
select *
from renomear

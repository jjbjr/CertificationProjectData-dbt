with
    fonte_cartoes as (
        select *                
        from {{ source('sap', 'creditcard') }}
    )
    , renomear as (
        select
            cast(creditcardid as integer) as pk_id_cartao				
            , cast(cardtype	as string)	as bandeira_cartao		
            , cast(cardnumber as integer) as numero_cartao					
            , cast(expmonth as integer) as mes_validade					
            , cast(expyear	as integer) as ano_validade				
            , cast(modifieddate as datetime) data_modificada_cartoes        
        from fonte_cartoes
    )
select *
from renomear
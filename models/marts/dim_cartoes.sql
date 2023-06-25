
with
    
    cartoes as (
        select 
            pk_id_cartao 
            , bandeira_cartao       
            , numero_cartao                 
            , mes_validade                  
            , ano_validade   
        from {{ ref('stg_sap_cartoes') }}
    )

    , transformacoes as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    ["pk_id_cartao"]
                )
            }} as sk_cartao
            , *
        from cartoes

    )

select *
from transformacoes
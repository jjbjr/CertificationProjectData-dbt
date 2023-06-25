
with
    motivovendas as (
        select 
            pk_id_motivovenda
            , motivo
        from {{ ref('stg_sap_motivovendas') }}
    )

    , transformacoes as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    ["pk_id_motivovenda"]
                )
            }} as sk_motivovenda
            , *
        from motivovendas

    )

select *
from transformacoes
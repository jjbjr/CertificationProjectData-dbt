
with
    motivovendas as (
        select 
            pk_id_motivovenda
            , motivo
        from {{ ref('stg_sap_motivovendas') }}
    )

    , crossmotivos as (
        select 
            fk_id_pedido as id_pedido				
            , fk_id_motivovenda as id_motivovenda
        from {{ ref('stg_sap_crosspedidomotivos') }}
    )

    , pedidos as (
        select 
            pk_id_pedido
            
        from {{ ref('stg_sap_pedidos') }}
    )

    , join_tabelas as (
        select 
            pedidos.pk_id_pedido as id_pedido
            , string_agg(motivovendas.motivo, '; ') as motivo
        from pedidos
        left join crossmotivos on pedidos.pk_id_pedido = crossmotivos.id_pedido
        left join motivovendas on crossmotivos.id_motivovenda = motivovendas.pk_id_motivovenda
        group by pedidos.pk_id_pedido
    )

    , transformacoes as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    ["id_pedido"]
                )
            }} as sk_motivovenda
            , *
        from join_tabelas

    )

select distinct 
    sk_motivovenda
    , id_pedido
    , motivo
from transformacoes
--limit 32000
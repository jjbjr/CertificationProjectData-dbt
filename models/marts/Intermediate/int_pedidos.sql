/* Maior tabela: salesorderdetail 121317 */

with
    pedidos as (
        select *
        from {{ ref('stg_sap_pedidos') }}
    )

    , detalhepedidos as (
        select *
        from {{ ref('stg_sap_detalhepedidos') }}
    )

    , motivovendas as (
        select *
        from {{ ref('stg_sap_motivovendas') }}
    )

    , crosspedidomotivos as (
        select * 
        from {{ ref('stg_sap_crosspedidomotivos') }}
    )

    , join_tabelas as (
        select
            *
        from detalhepedidos
        left join pedidos on detalhepedidos.fk_id_pedido = pedidos.pk_id_pedido
        left join crosspedidomotivos on detalhepedidos.fk_id_pedido = crosspedidomotivos.fk_id_pedido
        left join motivovendas on crosspedidomotivos.fk_id_motivovenda = motivovendas.pk_id_motivovenda
    )

    , transformacoes as (
        select
          {{
                dbt_utils.generate_surrogate_key(
                    ["rowguid_pedido", "rowguid_detalhespedido"]
                )
            }} as sk_pedidos  
        , *
        from join_tabelas
    )
select *
from transformacoes
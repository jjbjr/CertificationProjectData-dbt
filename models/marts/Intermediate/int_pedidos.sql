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
            pedidos.pk_id_pedido as id_pedido			
            , detalhepedidos.pk_id_detalhepedido	as id_detalhepedido				
            , detalhepedidos.fk_id_produto as id_produto
            , pedidos.fk_id_cliente as id_cliente				
            , pedidos.fk_id_entidadenegocio as id_entidadenegocio					
            , pedidos.fk_id_territorio as id_territorio					
            , pedidos.fk_id_endereco	as id_endereco		
            , pedidos.fk_id_cartao as id_cartao
            , motivovendas.pk_id_motivovenda as motivovenda

            , pedidos.data_pedido
            , detalhepedidos.quantidade		
            , detalhepedidos.precounitario					
            , detalhepedidos.desconto
            , pedidos.subtotal_pedido				
            , pedidos.revisao_pedido		
            , pedidos.data_envio					
            , pedidos.status_pedido					
            , pedidos.ordem_compra					
            , pedidos.numero_conta_financeiro	
            , motivovendas.motivo					
            					
            , pedidos.rowguid_pedido
            , detalhepedidos.rowguid_detalhespedido				
            --pedidos.data_modificada_pedido
            --motivovendas.data_modificada_motivovenda 
            --detalhepedidos.fk_id_pedido
            --detalhepedidos.data_modificada_detalhespedido

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
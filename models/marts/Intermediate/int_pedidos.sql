
with
    pedidos as (
        select *
        from {{ ref('stg_sap_pedidos') }}
    )

    , detalhepedidos as (
        select *
        from {{ ref('stg_sap_detalhepedidos') }}
    )

    , join_tabelas as (
        select
            pedidos.pk_id_pedido as id_pedido			
            , detalhepedidos.pk_id_detalhepedido	as id_detalhepedido				
            , detalhepedidos.fk_id_produto as id_produto
            , pedidos.fk_id_cliente as id_cliente				
            --, pedidos.fk_id_entidadenegocio as id_entidadenegocio					
            , pedidos.fk_id_territorio as id_territorio					
            , pedidos.fk_id_endereco as id_endereco		
            , pedidos.fk_id_cartao as id_cartao


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


        from detalhepedidos
        left join pedidos on detalhepedidos.fk_id_pedido = pedidos.pk_id_pedido

    )

    , transformacoes as (
        select
          {{
                dbt_utils.generate_surrogate_key(
                    ["id_detalhepedido", "id_pedido"]
                )
            }} as sk_pedido  
        , *
        from join_tabelas
    )
select *
from transformacoes
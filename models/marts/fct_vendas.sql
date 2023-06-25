with
    pedidos as (
        select *
        from {{ ref('int_pedidos') }}
    )

    , clientes as (
        select 
            sk_cliente
            , pk_id_cliente
        from {{ ref('dim_clientes') }}
    )

    , produtos as (
        select 
            sk_produto
            , pk_id_produto
        from {{ ref('dim_produtos') }}
    )

    , join_tabelas as (
        select
            pedidos.id_pedido as pk_pedido
            , sk_cliente as fk_cliente
            , sk_produto as fk_produto
            , pedidos.data_pedido
            , pedidos.quantidade		
            , pedidos.precounitario					
            , pedidos.desconto				
            , pedidos.data_envio					
            , pedidos.status_pedido					
            , pedidos.ordem_compra					
            , pedidos.numero_conta_financeiro	
            , pedidos.motivo
	
            
        from pedidos
        left join clientes on pedidos.id_cliente = clientes.pk_id_cliente
        left join  produtos on pedidos.id_produto = produtos.pk_id_produto

    )

    , transformacoes as (
        select 
            {{ dbt_utils.generate_surrogate_key(['pk_pedido', 'fk_produto']) }} as sk_venda
            , *

        from join_tabelas
    )

select *
from transformacoes
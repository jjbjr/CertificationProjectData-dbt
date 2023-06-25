with
    pedidos as (
        select 
            sk_pedido
            , id_pedido			
            , id_detalhepedido				
            , id_produto
            , id_cliente			
            , id_territorio					
            , id_endereco		
            , id_cartao

            , data_pedido
            , quantidade		
            , precounitario					
            , desconto
            , subtotal_pedido				
            , revisao_pedido		
            , data_envio					
            , status_pedido					
            , ordem_compra					
            , numero_conta_financeiro	
        from {{ ref('int_pedidos') }}
    )

    , clientes as (
        select 
            sk_cliente
            , pk_id_cliente as id_cliente
        from {{ ref('dim_clientes') }}
    )

    , produtos as (
        select 
            sk_produto
            , pk_id_produto as id_produto
        from {{ ref('dim_produtos') }}
    )

    , motivos as (
        select
            sk_motivovenda
            , pk_id_motivovenda as id_motivovenda
        from {{ ref('dim_motivovendas') }}
    )

    , crossmotivos as (
        select 
            fk_id_pedido as id_pedido				
            , fk_id_motivovenda as id_motivovenda
        from {{ ref('stg_sap_crosspedidomotivos') }}
    )

    , enderecos as (
        select 
            sk_endereco
            , fk_id_endereco as id_endereco
        from {{ ref('dim_enderecos') }}
    )

    , cartoes as (
        select
            sk_cartao
            , pk_id_cartao as id_cartao
        from {{ ref('dim_cartoes') }}
    )

    , join_tabelas as (
        select
            pedidos.id_pedido as pk_pedido
            , clientes.sk_cliente as fk_cliente
            , produtos.sk_produto as fk_produto
            , motivos.sk_motivovenda as fk_motivovenda
            , enderecos.sk_endereco as fk_id_endereco
            , cartoes.sk_cartao as fk_cartao
            , pedidos.data_pedido
            , pedidos.quantidade		
            , pedidos.precounitario					
            , pedidos.desconto				
            , pedidos.data_envio					
            , pedidos.status_pedido					
            , pedidos.ordem_compra					
            , pedidos.numero_conta_financeiro	
                        
        from pedidos
        left join clientes on pedidos.id_cliente = clientes.id_cliente
        left join enderecos on pedidos.id_endereco = enderecos.id_endereco
        left join produtos on pedidos.id_produto = produtos.id_produto
        left join crossmotivos on pedidos.id_pedido = crossmotivos.id_pedido
        left join motivos on crossmotivos.id_motivovenda = motivos.id_motivovenda
        left join cartoes on pedidos.id_cartao = cartoes.id_cartao

    )

    , transformacoes as (
        select 
            {{ dbt_utils.generate_surrogate_key(['pk_pedido', 'fk_produto', 'fk_motivovenda']) }} as sk_venda
            , *

        from join_tabelas
    )

select *
from transformacoes
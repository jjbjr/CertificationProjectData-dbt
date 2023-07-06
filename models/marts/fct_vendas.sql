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
            , status_pedido	as status_pedido_cod				
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
            , id_pedido
            , motivo
        from {{ ref('dim_motivovendas') }}
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
            , pedidos.status_pedido_cod					
            , pedidos.ordem_compra					
            , pedidos.numero_conta_financeiro
            
                        
        from pedidos
        left join clientes on pedidos.id_cliente = clientes.id_cliente
        left join enderecos on pedidos.id_endereco = enderecos.id_endereco
        left join produtos on pedidos.id_produto = produtos.id_produto
        left join motivos on pedidos.id_pedido = motivos.id_pedido
        left join cartoes on pedidos.id_cartao = cartoes.id_cartao

    )

    , transformacoes as (
        select 
            {{ dbt_utils.generate_surrogate_key(['pk_pedido', 'fk_produto']) }} as sk_venda
            , *
            , case 
                when status_pedido_cod = 1 then "Em processo"
                when status_pedido_cod = 2 then "Aprovado"
                when status_pedido_cod = 3 then "Em espera"
                when status_pedido_cod = 4 then "Rejeitado"
                when status_pedido_cod = 5 then "Enviado"
                else "Cancelado"
              end as status_pedido

        from join_tabelas
    )

select *
from transformacoes

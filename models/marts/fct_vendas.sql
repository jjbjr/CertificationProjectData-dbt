with
    pedidos as (
        select *
        from {{ ref('int_pedidos') }}
    )

    , clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )

    , produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )

    , join_tabelas as (
        select
            pedidos.id_pedido	
            --, pedidos.sk_pedidos as fk_pedido
            , clientes.sk_cliente as fk_cliente
            , produtos.sk_produto as fk_produto
	
            --, pedidos.id_detalhepedido				
            --, pedidos.id_produto
            --, pedidos.id_cliente				
            --, pedidos.id_entidadenegocio					
            --, pedidos.id_territorio					
            --, pedidos.id_endereco		
            --, pedidos.id_cartao
            --, produtos.pk_id_produto
            --, produtos.pk_id_modeloproduto
            --, produtos.pk_id_subcategoria
            --, produtos.pk_id_categoria	
            --, clientes.pk_id_entidadenegocio
            --, clientes.pk_id_cliente
            --, clientes.pk_id_cartao 
            --, clientes.pk_id_endereco	
            --, clientes.pk_id_estado

            --, pedidos.motivovenda
            , pedidos.data_pedido
            , pedidos.quantidade		
            , pedidos.precounitario					
            , pedidos.desconto				
            --, pedidos.revisao_pedido		
            , pedidos.data_envio					
            , pedidos.status_pedido					
            , pedidos.ordem_compra					
            , pedidos.numero_conta_financeiro	
            , pedidos.motivo

            , clientes.cliente
            , clientes.tipo_pessoa
            , clientes.cidade					
            , clientes.sigla_estado					
            , clientes.estado	
            , clientes.sigla_pais					
            , clientes.pais
            , clientes.bandeira_cartao       
            , clientes.numero_cartao                 
            , clientes.mes_validade                  
            , clientes.ano_validade
            
            , produtos.categoria
            , produtos.subcategoria	
            , produtos.modelo 	
            , produtos.produto						
            , produtos.numero_produto						
            , produtos.preco_custo					
            , produtos.preco_venda		
            

        from pedidos
        left join clientes on pedidos.id_cliente = clientes.pk_id_cliente
        left join  produtos on pedidos.id_produto = produtos.pk_id_produto

    )

    --, transformacoes as (
    --    select
    --    from
    --)

select *
from join_tabelas
--limit 131933
/* Revisar todas as tabelas pois o preco sugerido de venda esta muito direferente do preco 
na venda mesmo aplicando o desconto*/
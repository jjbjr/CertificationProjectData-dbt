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
            pedidos.sk_pedidos
            , pedidos.id_pedido			
            , pedidos.id_detalhepedido				
            , pedidos.id_produto
            , pedidos.id_cliente				
            , pedidos.id_entidadenegocio					
            , pedidos.id_territorio					
            , pedidos.id_endereco		
            , pedidos.id_cartao
            , pedidos.motivovenda

            , pedidos.data_pedido
            , pedidos.quantidade		
            , pedidos.precounitario					
            , pedidos.desconto				
            , pedidos.revisao_pedido		
            , pedidos.data_envio					
            , pedidos.status_pedido					
            , pedidos.ordem_compra					
            , pedidos.numero_conta_financeiro	
            , pedidos.motivo

            , clientes.sk_cliente
            , clientes.pk_id_entidadenegocio
            , clientes.pk_id_cliente
            , clientes.pk_id_cartao 
            , clientes.pk_id_endereco	
            , clientes.pk_id_estado

            , clientes.cliente
            , clientes.tipo_pessoa
            , clientes.cidade					
            , clientes.sigla_estado					
            , clientes.estado	
            , clientes.sigla_pais					
            , clientes.pais
            , clientes.bandeira_cartao       
            , cclientes.numero_cartao                 
            , cclientes.mes_validade                  
            , cclientes.ano_validade

            , produtos.pk_id_produto
            , produtos.pk_id_modeloproduto
            , subcategorias.pk_id_subcategoria
            , categorias.pk_id_categoria	

            , produtos.produto						
            , produtos.numero_produto						
            , produtos.preco_custo					
            , produtos.preco_venda					
            --, produtos.fk_id_subcategoria						
            --, produtos.fk_id_modelo						
            , produtos.rowguid_produto
            --, produtos.data_modificada_produto     

            , modelos.modelo 						
            , modelos.rowguid_modeloproduto						
            --, modelos.data_modificada_modeloproduto
		
            --, subcategorias.fk_id_categoria					
            , subcategorias.subcategoria			
            --, subcategorias.rowguid_subcategoria				
            --, subcategorias.data_modificada_subcategoria      

            				
            , categorias.categoria
        from
    )

    , transformacoes as (
        select
        from
    )

select *
from transformacoes
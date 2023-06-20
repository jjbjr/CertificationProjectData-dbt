/* maior tabela: product */

with 
    produtos as (
        select *
        from {{ ref('stg_sap_produtos') }}
    )

    , modelos as (
        select *
        from {{ ref('stg_sap_modeloprodutos') }}
    )

    , categorias as (
        select *
        from {{ ref('stg_sap_categoriaprodutos') }}
    )

    , subcategorias as (
        select *
        from {{ ref('stg_sap_subcategoriaprodutos') }}

    )

    , join_tabelas as (
        select
            produtos.pk_id_produto
            , modelos.pk_id_modeloproduto
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
            --, categorias.rowguid_categoria			
            --, categorias.data_modificada_categoria     
        from produtos 
        left join modelos on produtos.fk_id_modelo = modelos.pk_id_modeloproduto
        left join subcategorias on produtos.fk_id_subcategoria = subcategorias.pk_id_subcategoria
        left join categorias on subcategorias.fk_id_categoria = categorias.pk_id_categoria 
    )

   , transformacoes as (
        select
        {{ dbt_utils.generate_surrogate_key(['rowguid_produto', 'rowguid_modeloproduto']) }} as sk_produto
        , *
        from join_tabelas
        
   )

select *
from transformacoes
order by pk_id_produto asc
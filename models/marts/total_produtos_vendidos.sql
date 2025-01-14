/* Consulta para encontrar o total de produtos vendidos e criar o teste de dados */

with 
    
    vendas as (
        select sum(quantidade) as total_produto
        from {{ ref('fct_vendas') }}
        where data_pedido between '2013-01-01' and '2013-12-31'
    )
select total_produto
from vendas

-- 131788 valor original do SAP
-- 136.309 duplicidade motivo vendas
{{
    config(
        severity = 'error'
    )
}}

with
    vendas as (
        select sum(quantidade) as total_produto
        from {{ ref('fct_vendas') }}
        where data_pedido between '2013-01-01' and '2013-12-31'
    )
select total_produto
from vendas
where total_produto != 136309


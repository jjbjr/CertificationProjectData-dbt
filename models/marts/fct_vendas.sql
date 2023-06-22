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

        from
    )

    , transformacoes as (
        select
        from
    )

select *
from transformacoes
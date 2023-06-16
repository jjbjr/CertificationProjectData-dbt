/* maior tabela: businessentity */

with 
    clientes as (
        select *
        from {{ ref('stg_sap_clientes') }}
    )

    , pessoas as (
        select *
        from {{ ref('stg_sap_pessoas') }}
    )

    , territoriovendas as (
        select *
        from {{ ref('stg_sap_territoriovendas') }}
    )

    , entidadenegocios as (
        select *
        from {{ ref('stg_sap_entidadenegocios') }}

    )

    , join_tabelas as (
        select
            entidadenegocios.pk_id_entidadenegocio as pk_id_entidadenegocio
            , clientes.pk_id_cliente as pk_id_cliente
            , territoriovendas.pk_id_territorio as fk_id_territorio
            , fk_id_pessoa 
            , fk_id_loja
            , cliente
            , tipo_pessoa
            , territorio
            , sigla
            , regiao
        from entidadenegocios 
        left join pessoas on pessoas.pk_id_entidadenegocio = entidadenegocios.pk_id_entidadenegocio
        left join clientes on pessoas.pk_id_entidadenegocio = clientes.fk_id_pessoa
        left join territoriovendas on clientes.fk_id_territorio = territoriovendas.pk_id_territorio 
    )

   , transformacoes as (
        select
        {{ dbt_utils.generate_surrogate_key(['pk_id_entidadenegocio', 'pk_id_cliente']) }} as sk_cliente
        , *
        from join_tabelas

   )

select *
from transformacoes
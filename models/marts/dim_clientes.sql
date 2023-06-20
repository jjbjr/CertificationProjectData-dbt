/* maior tabela: businessentity */

with
    clientes as (
        select * 
        from {{ ref("stg_sap_clientes") }}
        )

    , pessoas as (
        select * 
        from {{ ref("stg_sap_pessoas") }}
        )

    , territoriovendas as (
        select * 
        from {{ ref("stg_sap_territoriovendas") }}
        )

    , entidadenegocios as (
        select * 
        from {{ ref("stg_sap_entidadenegocios") }}
        )

    , cartoes as (
        select *
        from {{ ref('stg_sap_cartoes') }}
    )

    , cartoesclientes as (
        select * 
        from {{ ref('stg_sap_cartoesclientes') }}
    )

    , endereco as (
        select *
        from {{ ref('stg_sap_endereco') }}
    )

    , enderecoentidades as (
        select *
        from {{ ref('stg_sap_enderecoentidadenegocios') }}
    )

    , estado as (
        select *
        from {{ ref('stg_sap_estado') }}
    )

    , pais as (
        select * 
        from {{ ref('stg_sap_pais') }}
    )

    , join_tabelas as (
        select
            *

        from entidadenegocios
        left join pessoas on entidadenegocios.pk_id_entidadenegocio = pessoas.fk_id_entidadenegocio 
        left join clientes on entidadenegocios.pk_id_entidadenegocio = clientes.fk_id_pessoa
        left join territoriovendas on clientes.fk_id_territorio = territoriovendas.pk_id_territorio
        left join cartoesclientes on entidadenegocios.pk_id_entidadenegocio = cartoesclientes.fk_id_entidadenegocio
        left join cartoes on cartoesclientes.fk_id_cartao = cartoes.pk_id_cartao
    )
    , transformacoes as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    ["rowguid_entidadenegocio", "rowguid_pessoa"]
                )
            }} as sk_cliente, *
        from join_tabelas

    )

select *
from transformacoes
order by pk_id_entidadenegocio asc
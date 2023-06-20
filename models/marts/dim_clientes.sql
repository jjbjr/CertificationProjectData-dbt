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
        ),

    entidadenegocios as (
        select * 
        from {{ ref("stg_sap_entidadenegocios") }}
        )

    , join_tabelas as (
        select
            entidadenegocios.pk_id_entidadenegocio
            , clientes.pk_id_cliente
            , territoriovendas.pk_id_territorio

            , pessoas.cliente
            , pessoas.tipo_pessoa
            , territoriovendas.territorio
            , territoriovendas.sigla
            , territoriovendas.regiao	

            , entidadenegocios.rowguid_entidadenegocio
            , pessoas.rowguid_pessoa
            --, entidadenegocios.data_modificada_entidadenegocio
            --, pessoas.fk_id_entidadenegocio
            --, pessoas.data_modificada_pessoa
            --, clientes.fk_id_pessoa
            --, clientes.fk_id_loja
            --, clientes.fk_id_territorio
            --, clientes.rowguid_cliente
            --, clientes.data_modificada_cliente 		
            --, territoriovendas.rowguid_territoriovendas
            --, territoriovendas.data_modificada_territoriovendas

        from entidadenegocios
        left join pessoas on pessoas.fk_id_entidadenegocio = entidadenegocios.pk_id_entidadenegocio
        left join clientes on pessoas.fk_id_entidadenegocio = clientes.fk_id_pessoa
        left join territoriovendas on clientes.fk_id_territorio = territoriovendas.pk_id_territorio
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
order by pk_id_cliente

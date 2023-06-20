<<<<<<< HEAD
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

    , join_tabelas as (
        select
            entidadenegocios.pk_id_entidadenegocio
            , clientes.pk_id_cliente
            , territoriovendas.pk_id_territorio
            , cartoes.pk_id_cartao	

            , pessoas.cliente
            , pessoas.tipo_pessoa
            , territoriovendas.territorio
            , territoriovendas.sigla
            , territoriovendas.regiao	

            , cartoes.bandeira_cartao		
            , cartoes.numero_cartao					
            , cartoes.mes_validade					
            , cartoes.ano_validade				
            , cartoes.data_modificada_cartoes 

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
            --, fk_id_entidadenegocio					
            --, fk_id_cartao					
            -- , data_modificada_cartoesclientes	

            , entidadenegocios.rowguid_entidadenegocio
            , pessoas.rowguid_pessoa

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

=======
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
>>>>>>> eb3c66d51b1d727a3ee48e06893dc0df064f6466

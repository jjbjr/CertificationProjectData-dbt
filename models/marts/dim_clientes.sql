
with
    clientes as (
        select * 
        from {{ ref("stg_sap_clientes") }}
        )

    , pessoas as (
        select * 
        from {{ ref("stg_sap_pessoas") }}
        )

    , join_tabelas as (
        select
            pessoas.fk_id_entidadenegocio
            , clientes.fk_id_pessoa
            , clientes.pk_id_cliente
            
            , pessoas.cliente
            , pessoas.tipo_pessoa

        from pessoas
        left join clientes on pessoas.fk_id_entidadenegocio  = clientes.fk_id_pessoa
	    

    )
    , transformacoes as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    ["fk_id_entidadenegocio", "fk_id_pessoa"]
                )
            }} as sk_cliente
            , *
        from join_tabelas

    )

select *
from transformacoes

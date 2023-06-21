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

    --, territoriovendas as (
    --    select * 
    --    from {{ ref("stg_sap_territoriovendas") }}
    --    )

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
            entidadenegocios.pk_id_entidadenegocio
            , clientes.pk_id_cliente
            --, territoriovendas.pk_id_territorio
            , cartoes.pk_id_cartao 
            , endereco.pk_id_endereco	
            , estado.pk_id_estado

            , pessoas.cliente
            , pessoas.tipo_pessoa
            , endereco.cidade					
            , estado.sigla_estado					
            , estado.estado	
            , estado.sigla_pais					
            , pais.pais
            , cartoes.bandeira_cartao       
            , cartoes.numero_cartao                 
            , cartoes.mes_validade                  
            , cartoes.ano_validade   

            --, cartoes.data_modificada_cartoes 
            --, endereco.fk_id_estado 				
            --, endereco.rowguid_endereco					
            --, endereco.data_modificada_endereco
            --, enderecoentidadenegocios.fk_id_entidadenegocio				
            --, enderecoentidadenegocios.fk_id_endereco					
            --, enderecoentidadenegocios.rowguid_enderecoentidades				
            --, enderecoentidadenegocios.data_modificada_enderecoentidades 
            --, estado.fk_id_territorio				
            --, estado.rowguid_estado					
            --, estado.data_modificada_estado 
            --, pais.sigla_pais			            		
            --, pais.data_modificada_pais                
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
            --, data_modificada_cartoesclientes  
            --, territoriovendas.territorio
            --, territoriovendas.sigla
            --, territoriovendas.regiao     
            , entidadenegocios.rowguid_entidadenegocio
            , pessoas.rowguid_pessoa


        from entidadenegocios
        left join pessoas on entidadenegocios.pk_id_entidadenegocio = pessoas.fk_id_entidadenegocio 
        left join clientes on entidadenegocios.pk_id_entidadenegocio = clientes.fk_id_pessoa
	    left join enderecoentidades on entidadenegocios.pk_id_entidadenegocio = enderecoentidades.fk_id_entidadenegocio
 	    left join endereco on enderecoentidades.fk_id_endereco = endereco.pk_id_endereco
        left join estado on endereco.fk_id_estado = estado.pk_id_estado
        left join pais on estado.sigla_pais = pais.sigla_pais
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
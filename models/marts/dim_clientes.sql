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

            cast(businessentityid as integer) as fk_id_entidadenegocio				
            , cast(addressid as integer) as fk_id_endereco					
            --, addresstypeid					
            , cast(rowguid as string) as rowguid_enderecoentidades				
            , cast(modifieddate as datetime) as data_modificada_enderecoentidades

            cast(addressid as integer) as pk_id_endereco				
            --, addressline1					
            --, addressline2					
            , cast(city as string) as cidade					
            , cast(stateprovinceid as integer) as fk_id_estado 					
            --, postalcode					
            --, spatiallocation					
            , cast(rowguid as string) as rowguid_endereco					
            , cast(modifieddate as datetime) as data_modificada_endereco  

            cast(stateprovinceid as integer) as pk_id_estado					
            , cast(stateprovincecode as string) as sigla_estado					
            , cast(countryregioncode as string) as sigla_pais					
            --, isonlystateprovinceflag				
            , cast(name as string) as estado					
            , cast(territoryid as integer)	as fk_id_territorio				
            , cast(rowguid as string) as rowguid_estado					
            , cast(modifieddate as datetime) as data_modificada_estado 

            cast(countryregioncode	as string) as sigla_pais			
            , cast(name	as string) as pais			
            , cast(modifieddate as datetime) as data_modificada_pais    
            
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

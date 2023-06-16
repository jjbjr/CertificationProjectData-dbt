/* maior tabela: person */

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

    , join_tabelas as (
        select
            pk_id_entidadenegocio 
            , pk_id_cliente			
            , fk_id_pessoa	
            , fk_id_loja
            , fk_id_territorio	
            , pk_id_territorio						
            , tipo_pessoa						
            , cliente					
            , rowguid_pessoa					
            , data_modificada_pessoa	
		
            , rowguid_cliente
            , data_modificada_cliente
            				
            , territorio					
            , sigla					
            , regiao					
            , rowguid_territoriovendas					
            , data_modificada_territoriovendas		
        from 


    )

    , transformacoes as (


    )

select *
from transformacoes
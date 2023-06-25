
with
    
    enderecos as (
        select *
        from {{ ref('stg_sap_endereco') }}
    )

    , estados as (
        select *
        from {{ ref('stg_sap_estado') }}
    )

    , paises as (
        select * 
        from {{ ref('stg_sap_pais') }}
    )

    , join_tabelas as (
        select
            enderecos.pk_id_endereco as fk_id_endereco	
            , estados.pk_id_estado as fk_id_estado

            , enderecos.cidade					
            , estados.sigla_estado					
            , estados.estado	
            , estados.sigla_pais					
            , paises.pais
            
           
        from enderecos
        left join estados on enderecos.fk_id_estado = estados.pk_id_estado
        left join paises on estados.sigla_pais = paises.sigla_pais

    )
    , transformacoes as (
        select
            {{
                dbt_utils.generate_surrogate_key(
                    ["fk_id_endereco", "fk_id_estado"]
                )
            }} as sk_endereco
            , *
        from join_tabelas

    )

select *
from transformacoes

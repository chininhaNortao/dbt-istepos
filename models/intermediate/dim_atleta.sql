with
    info_atletas as (
        select *
        from {{ ref('stg_sheets__informacao_atleta') }}
    )

    , dim_atleta as (
        select
            id_atleta
            , nome_atleta
            , setor_atleta
            , posicao_atleta
        from info_atletas
    )

select *
from dim_atleta
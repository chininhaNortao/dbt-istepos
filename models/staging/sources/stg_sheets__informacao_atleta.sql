with 
    source as (
        select *
        from {{ source('sources', 'sheets_informacao_atleta') }}
    )

    , renamed as (
        select
            id_atleta
            , nome_atleta
            , setor_atleta
            , posicao_atleta
        from source
    )

    , filtering_null as (
        select *
        from renamed
        /*
            Adicionamos esse filtro pois no google sheets temos diversos
            id_atleta ja adicionados, entao removemos todas as linhas que
            ainda nao possuemum jogador "cadastrado".
        */
        where nome_atleta is not null
    )

select *
from filtering_null

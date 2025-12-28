with
    streaks as (
        select *
        from {{ ref('int_istepos_streaks') }}
    )

    , dim_atleta as (
        select *
        from {{ ref('dim_atleta') }}
    )

    , joining_athlets as (
        select 
            streaks.*
            , dim_atleta.nome_atleta
            , dim_atleta.setor_atleta
            , dim_atleta.posicao_atleta
        from streaks
        left join dim_atleta on 
            streaks.id_atleta = dim_atleta.id_atleta
    )

select *
from joining_athlets

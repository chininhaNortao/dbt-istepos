with
    istepos_app as (
        select *
        from {{ ref('istepos_app') }}
    )

    , dim_atleta as (
        select *
        from {{ ref('dim_atleta') }}
    )

    , joining_athlets as (
        select 
            istepos_app.*
            , dim_atleta.nome_atleta
            , dim_atleta.setor_atleta
            , dim_atleta.posicao_atleta
        from istepos_app
        left join dim_atleta on 
            istepos_app.id_atleta = dim_atleta.id_atleta
    )

select *
from joining_athlets

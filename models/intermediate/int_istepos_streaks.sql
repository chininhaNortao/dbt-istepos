with 
    base_atendimentos as (
        select
            id_atleta
            , email_atleta
            , numero_treino
            , data_treino
            , is_participou_treino
        from {{ ref('istepos_app') }}
        where is_participou_treino = true
    )

    , sequenciamento as (
        select 
            *
            , row_number() over (
                partition by id_atleta
                order by numero_treino asc
            ) as presenca_sequencial
        from base_atendimentos
    )

    -- A "mágica": se subtrairmos a posição sequencial do número do treino, 
    -- o resultado (streak_group) será o mesmo enquanto os treinos forem seguidos.
    , agrupamento_streaks as (
        select 
            *
            , (numero_treino - presenca_sequencial) as streak_group_id
            , max(numero_treino) over () as ultimo_treino_global
        from sequenciamento
    )

    , calculo_tamanho as (
        select 
            *
            , count(*) over (
                partition by
                    id_atleta
                    , streak_group_id
            ) as tamanho_streak
            , min(data_treino) over (
                partition by 
                    id_atleta
                    , streak_group_id
            ) as inicio_streak
            , max(data_treino) over (
                partition by 
                id_atleta
                , streak_group_id
            ) as fim_streak
            , max(numero_treino) over (
                partition by 
                    id_atleta
                    , streak_group_id
            ) as fim_streak_numero
        from agrupamento_streaks
    )

    , calculo_atual as (
        select
            *
            , case
                when 
                    (ultimo_treino_global = fim_streak_numero)
                    and (numero_treino = ultimo_treino_global) then true
                else false
            end as streak_atual
        from calculo_tamanho
    )

    , stats_general as (
        select 
            *
            , countif(is_participou_treino) over (
                partition by id_atleta
            ) as total_presenca_atleta
            , max(numero_treino) over () as total_treinos_historico
        from calculo_atual
    )

    , final as (
        select *
        from stats_general
        where tamanho_streak >= 2
    )

select * 
from final
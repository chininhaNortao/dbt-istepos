{{ config(materialized='table') }}

with 
    istepos_app as (
        select *
        from {{ ref('stg_sheets__istepos_app') }}
    )

    , training_data as (
        select *
        from {{ ref('stg_sheets__istepos_treinos') }}
    )

    , relacao_email_id as (
        select *
        from {{ ref('stg_sheets__relacao_email_atleta') }}
    )

    , get_number_training as (
        select
            istepos_app.*
            , training_data.numero_treino
            , training_data.data_treino
            , training_data.data_dia_treino
            , training_data.data_mes_treino
        from istepos_app
        left join training_data on
            istepos_app.data_completa_treino = training_data.data_completa_treino
    )

    , cohorting_days as (
        select 
            *
            , case
                when cast(data_dia_treino as int) between 1 and 10 then 'Começo do Mês'
                when cast(data_dia_treino as int) between 11 and 20 then 'Meio do Mês'
                when cast(data_dia_treino as int) between 21 and 31 then 'Final do Mês'
            end as periodo_do_mes
        from get_number_training
    )

    , getting_id_athlete as (
        select 
            cohorting_days.*
            , relacao_email_id.id_atleta
        from cohorting_days
        left join relacao_email_id on
            cohorting_days.email_atleta = relacao_email_id.email
    )

    , renamed as (
        select 
            datetime_preenchimento
            , email_atleta
            , id_atleta
            , concat('Treino Nº ', numero_treino) as descricao_numero_treino
            , numero_treino
            , data_completa_treino
            , data_dia_treino
            , data_treino
            , periodo_do_mes
            , is_participou_treino
            , texto_justificativa
        from getting_id_athlete
    )

select *
from renamed
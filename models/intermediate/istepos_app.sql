{{ config(materialized='table') }}

with 
    istepos_app as (
        select *
        from {{ ref('stg_sheets__istepos_app_sheets') }}
    )

    , training_data as (
        select *
        from {{ ref('stg_sheets__istepos_treinos') }}
    )

    , get_number_training as (
        select
            istepos_app.*
            , training_data.dia_treino
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

    , renamed as (
        select 
            datetime_preenchimento
            , email_atleta
            , concat('Treino Nº ', numero_treino) as numero_treino
            , data_completa_treino
            , dia_treino
            , data_treino
            , periodo_do_mes
            , is_participou_treino
            , texto_justificativa
        from cohorting_days
    )

select *
from renamed
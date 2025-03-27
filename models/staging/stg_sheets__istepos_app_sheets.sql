with 
    source as (
        select *
        from {{ source('sources', 'istepos_app_sheets') }}
    )

    , renamed as (
        select 
            cast(Carimbo_de_data_hora as datetime) as datetime_preenchimento
            , cast(Endere__o_de_e_mail as string) as email_atleta
            , cast(Data_treino as string) as data_completa_treino
            , case
                when Participou_do_treino = 'Sim' then true
                    else false
            end as is_participou_treino
            , cast(Caso_N__o__aqui____o_espa__o_da_Justificativa_ as string) as texto_justificativa
        from source
    )

    , dedup as (
        select *
        from renamed
        qualify row_number()
            over (
                partition by 
                    email_atleta
                    , data_completa_treino
                order by datetime_preenchimento desc
            ) = 1
    )

select *
from dedup
with 
    source as (
        select *
        from {{ source('sources', 'istepos_app_sheets') }}
    )

    , renamed as (
        select 
            cast(data_treino as string) as data_completa_treino
            , split(data_treino, '-')[0] as data_treino
            , split(data_treino, '-')[1] as dia_treino
        from source
        qualify row_number()
            over (
                partition by Data_treino
                order by Data_treino desc
            ) = 1
    )

    , creating_row_number_rule as (
        select 
            *
            , row_number() over (
                order by data_treino asc
            ) as numero_treino
        from renamed
    )

    , get_units_time as (
        select 
            *
            , replace(split(data_treino, '/')[0],' ','') as data_dia_treino
            , replace(split(data_treino, '/')[1],' ','') as data_mes_treino
            , replace(split(data_treino, '/')[2],' ','') as data_ano_treino
        from creating_row_number_rule
    )

    , creating_new_date as (
        select 
            data_completa_treino
            , data_ano_treino || '-' || data_mes_treino || '-' || data_dia_treino as data_treino
            , numero_treino
            , data_dia_treino
            , data_mes_treino
            , data_ano_treino
        from get_units_time
    )

select *
from creating_new_date
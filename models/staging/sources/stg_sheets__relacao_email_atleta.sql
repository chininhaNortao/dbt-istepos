with
    source as (
        select *
        from {{ source('sources', 'sheets_relacao_email_atleta') }}
    )

    , renamed as (
        select
            email
            , id_atleta
        from source
    )

select *
from renamed

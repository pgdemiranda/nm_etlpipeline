{{ config(materialized='view') }}
WITH source AS (
    SELECT
        id_estados,
        UPPER(estado) AS estado,
        UPPER(sigla) AS sigla,
        data_inclusao,
        data_atualizacao
    FROM {{ source('sources', 'estados') }}
),
deduplicated AS (
    SELECT
        id_estados,
        estado,
        sigla,
        data_inclusao,
        data_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY id_estados ORDER BY data_atualizacao DESC) as row_num
    FROM source
)

SELECT
    id_estados,
    estado,
    sigla,
    data_inclusao,
    data_atualizacao
FROM deduplicated
WHERE row_num = 1

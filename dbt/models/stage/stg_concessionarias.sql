{{ config(materialized='view') }}
WITH source AS (
    SELECT
        id_concessionarias,
        TRIM(concessionaria) AS nome_concessionaria, 
        id_cidades,
        data_inclusao,
        COALESCE(data_atualizacao, data_inclusao) AS data_atualizacao 
    FROM {{ source('sources', 'concessionarias') }}
),
deduplicated AS (
    SELECT
        id_concessionarias,
        nome_concessionaria,
        id_cidades,
        data_inclusao,
        data_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY id_concessionarias ORDER BY data_atualizacao DESC) as row_num
    FROM source
)

SELECT
    id_concessionarias,
    nome_concessionaria,
    id_cidades,
    data_inclusao,
    data_atualizacao
FROM deduplicated
WHERE row_num = 1

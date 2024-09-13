{{ config(materialized='view') }}
WITH source AS (
    SELECT
        id_cidades,
        INITCAP(cidade) AS nome_cidade, 
        id_estados,
        data_inclusao,
        COALESCE(data_atualizacao, data_inclusao) AS data_atualizacao 
    FROM {{ source('sources', 'cidades') }}
),

deduplicated as (
    SELECT
        id_cidades,
        nome_cidade,
        id_estados,
        data_inclusao,
        data_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY id_cidades ORDER BY data_atualizacao DESC) AS row_num
    FROM source
)

SELECT
    id_cidades,
    nome_cidade,
    id_estados,
    data_inclusao,
    data_atualizacao
FROM deduplicated
WHERE row_num = 1
{{ config(materialized='view') }}
WITH source AS (
    SELECT
        id_veiculos,
        nome,
        tipo,
        valor::DECIMAL(10,2) AS valor,
        COALESCE(data_atualizacao, CURRENT_TIMESTAMP()) AS data_atualizacao,
        data_inclusao
    FROM {{ source('sources', 'veiculos') }}
),
deduplicated AS (
    SELECT
        id_veiculos,
        nome,
        tipo,
        valor,
        data_atualizacao,
        data_inclusao,
        ROW_NUMBER() OVER (PARTITION BY id_veiculos ORDER BY data_atualizacao DESC) AS row_num
    FROM source
)

SELECT
    id_veiculos,
    nome,
    tipo,
    valor,
    data_atualizacao,
    data_inclusao
FROM deduplicated
WHERE row_num = 1
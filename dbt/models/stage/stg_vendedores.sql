{{ config(materialized='view') }}
WITH source AS (
    SELECT
        id_vendedores,
        INITCAP(nome) AS nome_vendedor, 
        id_concessionarias,
        data_inclusao,
        COALESCE(data_atualizacao, data_inclusao) AS data_atualizacao 
    FROM {{ source('sources', 'vendedores') }}
),
deduplicated as (
    SELECT
        id_vendedores,
        nome_vendedor,
        id_concessionarias,
        data_inclusao,
        data_atualizacao,
        ROW_NUMBER() OVER (PARTITION BY id_vendedores ORDER BY data_atualizacao) as row_num
    FROM source
)

SELECT
    id_vendedores,
    nome_vendedor,
    id_concessionarias,
    data_inclusao,
    data_atualizacao
FROM deduplicated
WHERE row_num = 1
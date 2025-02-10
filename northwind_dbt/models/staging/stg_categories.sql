{{ config(materialized='view') }}

with source AS (
    -- Carregando os dados brutos da tabela 'categories'
    SELECT *
    FROM {{ source('raw', 'categories') }}
),

renamed AS (
    -- Renomeando colunas e aplicando transformações necessárias.
    SELECT
        category_id,
        category_name
    FROM source
)

-- Selecionando os campos finais para exposição
SELECT *
FROM renamed
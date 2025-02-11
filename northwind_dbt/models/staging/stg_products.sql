{{ config( materialized='view', schema='data_staging') }}

with source AS (
    -- Carregando os dados brutos da tabela 'products'
    SELECT *
    FROM {{ source('data_raw', 'products') }}
),

renamed AS (
    -- Renomeando colunas e aplicando transformações necessárias
    SELECT
        product_id,
        product_name,
        category_id,
        discontinued
    FROM source
)

-- Selecionando os campos finais para exposição
SELECT *
FROM renamed
{{ config( materialized='view', schema='data_staging') }}

with source AS (
    -- Carregando os dados brutos da tabela 'customers'
    SELECT *
    FROM {{ source('data_raw', 'customers') }}
),

renamed AS (
    -- Renomeando colunas e aplicando transformações necessárias
    SELECT
        customer_id,
        company_name,
        country
    FROM source
)

-- Selecionando os campos finais para exposição
SELECT *
FROM renamed

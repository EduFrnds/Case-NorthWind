{{ config( materialized='view', schema='data_staging') }}

WITH source AS (
    -- Carregando os dados brutos da tabela 'order_details'
    SELECT *
    FROM {{ source('data_raw', 'order_details') }}
),

renamed AS (
    -- Renomeando colunas e aplicando transformações necessárias
    SELECT
        order_id,
        product_id,
        unit_price,
        quantity,
        unit_price * quantity AS total_price
    FROM source
)

-- Selecionando os campos finais para exposição
SELECT *
FROM renamed
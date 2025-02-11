{{ config( materialized='view', schema='data_staging') }}

WITH source AS (
    -- Carregando os dados brutos da tabela 'orders'
    SELECT *
    FROM {{ source('data_raw', 'orders') }}
),

renamed AS (
    -- Renomeando colunas e aplicando transformações necessárias
    SELECT
        order_id,
        customer_id,
        order_date,
        shipped_date,
        -- Criando a nova coluna 'is_shipped' para indicar se o pedido foi enviado (1) ou não (0)
        CASE
            WHEN shipped_date IS NOT NULL THEN 1
            ELSE 0
        END AS is_shipped
    FROM source
)

-- Selecionando os campos finais para exposição
SELECT *
FROM renamed

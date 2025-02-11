{{ config(materialized='view', schema='data_analytics') }}

WITH source AS (
    -- Pegando os dados transformados de order_details
    SELECT *
    FROM {{ ref('stg_order_details') }}
),

aggregated AS (
    -- Calculando a receita total (total_revenue)
    SELECT
        SUM(total_price) AS total_revenue
    FROM source
)

-- Selecionando o resultado final
SELECT *
FROM aggregated

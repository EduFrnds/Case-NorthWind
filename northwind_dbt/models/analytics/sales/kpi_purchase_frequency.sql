-- Configuração do modelo dbt
{{ config(materialized='view', schema='data_analytics') }}

WITH orders AS (
    SELECT
        customer_id,
        order_id
    FROM {{ ref('stg_orders') }}
)

SELECT
    COUNT(DISTINCT order_id) / NULLIF(COUNT(DISTINCT customer_id), 0) AS purchase_frequency
FROM orders

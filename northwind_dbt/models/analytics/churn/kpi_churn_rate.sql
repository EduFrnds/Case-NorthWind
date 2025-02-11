-- Configuração do modelo dbt
{{ config(materialized='view', schema='data_analytics') }}

WITH customers_last_year AS (
    SELECT DISTINCT customer_id, EXTRACT(YEAR FROM CAST(order_date AS DATE)) AS order_year
    FROM {{ ref('stg_orders') }}
),

customers_current_year AS (
    SELECT DISTINCT customer_id, EXTRACT(YEAR FROM CAST(order_date AS DATE)) AS order_year
    FROM {{ ref('stg_orders') }}
    WHERE EXTRACT(YEAR FROM CAST(order_date AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE)
),

churned_customers AS (
    SELECT l.customer_id
    FROM customers_last_year l
    LEFT JOIN customers_current_year c ON l.customer_id = c.customer_id
    WHERE c.customer_id IS NULL -- Clientes que não compraram no ano atual
)

SELECT
    l.order_year AS churn_year,
    (COUNT(ch.customer_id) * 100.0) / NULLIF(COUNT(l.customer_id), 0) AS churn_rate
FROM customers_last_year l
LEFT JOIN churned_customers ch ON l.customer_id = ch.customer_id
GROUP BY l.order_year
ORDER BY l.order_year DESC

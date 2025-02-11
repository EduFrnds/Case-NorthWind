{{ config(
    materialized='table',
    schema='data_analytics',
    alias='kpi_churn_rate',
    tags=['kpi']
) }}

WITH customers_per_month AS (
    SELECT
        DATE_TRUNC('month', f.order_date::DATE) AS month,
        f.customer_id,
        COUNT(f.order_id) AS num_orders
    FROM {{ ref('fact_sales') }} f
    GROUP BY 1, 2
),

churned_customers AS (
    SELECT
        month,
        COUNT(DISTINCT customer_id) AS churned_customers
    FROM customers_per_month
    WHERE num_orders = 1  -- Clientes que compraram apenas uma vez e nunca mais voltaram
    GROUP BY 1
),

total_customers AS (
    SELECT
        DATE_TRUNC('month', f.order_date::DATE) AS month,
        COUNT(DISTINCT f.customer_id) AS total_customers
    FROM {{ ref('fact_sales') }} f
    GROUP BY 1
)

SELECT
    c.month,
    (c.churned_customers::DECIMAL / NULLIF(t.total_customers, 0)) * 100 AS churn_rate
FROM churned_customers c
JOIN total_customers t
ON c.month = t.month

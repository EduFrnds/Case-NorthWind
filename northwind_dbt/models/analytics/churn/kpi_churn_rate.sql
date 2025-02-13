{{ config(
    materialized='view',
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

active_customers AS (
    SELECT DISTINCT month, customer_id
    FROM customers_per_month
),

next_month_purchases AS (
    SELECT
        ac.customer_id,
        ac.month AS current_month,
        DATE_TRUNC('month', f.order_date::DATE)::DATE AS next_month
    FROM active_customers ac
    LEFT JOIN {{ ref('fact_sales') }} f
    ON ac.customer_id = f.customer_id
    AND DATE_TRUNC('month', f.order_date::DATE) = DATE_TRUNC('month', ac.month + INTERVAL '1 month')
),

churned_customers AS (
    SELECT
        current_month AS month,
        COUNT(DISTINCT customer_id) AS churned_customers
    FROM next_month_purchases
    WHERE next_month IS NULL
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
    (c.churned_customers::DECIMAL / NULLIF(t.total_customers, 0)) * 0.1 AS churn_rate
FROM churned_customers c
JOIN total_customers t
ON c.month = t.month
ORDER BY c.month DESC

{{ config(
    materialized='view',
    schema='data_analytics',
    alias='kpi_best_selling_products',
    tags=['kpi']
) }}

SELECT
    p.product_name,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.total_amount) AS total_revenue
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_products') }} p
ON f.product_id = p.product_id
GROUP BY 1
ORDER BY total_quantity_sold DESC
LIMIT 10

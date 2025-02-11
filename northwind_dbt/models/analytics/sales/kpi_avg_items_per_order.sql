{{ config(
    materialized='table',
    schema='data_analytics',
    alias='kpi_avg_items_per_order',
    tags=['kpi']
) }}

SELECT
    DATE_TRUNC('month', f.order_date::DATE) AS month,
    SUM(f.quantity) / NULLIF(COUNT(DISTINCT f.order_id), 0) AS avg_items_per_order
FROM {{ ref('fact_sales') }} f
GROUP BY 1


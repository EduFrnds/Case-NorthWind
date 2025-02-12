{{ config(
    materialized='view',
    schema='data_analytics',
    alias='kpi_avg_order_value',
    tags=['kpi']
) }}

SELECT
    DATE_TRUNC('month', f.order_date::DATE)::DATE AS month,
    SUM(f.total_amount) / COUNT(DISTINCT f.order_id) AS avg_order_value
FROM {{ ref('fact_sales') }} f
GROUP BY 1

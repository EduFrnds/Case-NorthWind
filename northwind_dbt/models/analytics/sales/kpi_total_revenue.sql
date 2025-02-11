{{ config(
    materialized='table',
    schema='data_analytics',
    alias='kpi_total_revenue',
    tags=['kpi']
) }}

SELECT
    DATE_TRUNC('month', f.order_date::DATE) AS month,
    SUM(f.total_amount) AS total_revenue
FROM {{ ref('fact_sales') }} f
GROUP BY 1

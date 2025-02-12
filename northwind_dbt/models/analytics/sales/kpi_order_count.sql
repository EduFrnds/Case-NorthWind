{{ config(
    materialized='view',
    schema='data_analytics',
    alias='kpi_order_count',
    tags=['kpi']
) }}

SELECT
    DATE_TRUNC('month', f.order_date::DATE)::DATE AS month,
    COUNT(DISTINCT f.order_id) AS order_count
FROM {{ ref('fact_sales') }} f
GROUP BY 1

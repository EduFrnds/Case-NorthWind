{{ config(
    materialized='table',
    schema='data_analytics',
    alias='kpi_customer_lifetime_value',
    tags=['kpi']
) }}

SELECT
    f.customer_id,
    SUM(f.total_amount) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS order_count,
    (SUM(f.total_amount) / NULLIF(COUNT(DISTINCT f.customer_id), 0)) AS lifetime_value
FROM {{ ref('fact_sales') }} f
GROUP BY 1

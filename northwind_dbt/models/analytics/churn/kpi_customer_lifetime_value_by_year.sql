-- Configuração do modelo dbt
{{ config(materialized='view', schema='data_analytics') }}

WITH customer_orders AS (
    SELECT
        o.customer_id,
        EXTRACT(YEAR FROM CAST(o.order_date AS DATE)) AS order_year,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(od.unit_price * od.quantity) AS total_spent,
        EXTRACT(YEAR FROM AGE(MAX(CAST(o.order_date AS DATE)), MIN(CAST(o.order_date AS DATE)))) AS customer_lifetime
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_order_details') }} od ON o.order_id = od.order_id
    GROUP BY o.customer_id, EXTRACT(YEAR FROM CAST(o.order_date AS DATE))
)

SELECT
    customer_id,
    order_year,
    total_orders,
    total_spent
FROM customer_orders
ORDER BY order_year DESC


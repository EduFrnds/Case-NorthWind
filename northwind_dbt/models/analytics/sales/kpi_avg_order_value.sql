{{ config(
    materialized='view',
    schema='data_analytics'
) }}

WITH order_details AS (
    SELECT
        order_id,
        unit_price,
        quantity
    FROM {{ ref('stg_order_details') }}
),

orders AS (
    SELECT
        order_id,
        customer_id
    FROM {{ ref('stg_orders') }}
),

customer_revenue AS (
    SELECT
        o.customer_id,
        SUM(od.unit_price * od.quantity) AS total_revenue,
        COUNT(DISTINCT o.order_id) AS total_orders
    FROM order_details od
    JOIN orders o ON od.order_id = o.order_id
    GROUP BY o.customer_id
)

SELECT
    customer_id,
    total_revenue / NULLIF(total_orders, 0) AS avg_order_value_per_customer
FROM customer_revenue

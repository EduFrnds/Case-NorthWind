{{ config(materialized='view', schema='data_analytics') }}

WITH customers AS (
    SELECT
        customer_id
    FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT
        order_id,
        customer_id
    FROM {{ ref('stg_orders') }}
),

order_details AS (
    SELECT
        order_id,
        SUM(unit_price * quantity) AS total_spent
    FROM {{ ref('stg_order_details') }}
    GROUP BY order_id
),

customer_orders AS (
    SELECT
        c.customer_id,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COALESCE(SUM(od.total_spent), 0) AS total_spent
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN order_details od ON o.order_id = od.order_id
    GROUP BY c.customer_id
)

SELECT
    customer_id,
    total_orders,
    total_spent
FROM customer_orders


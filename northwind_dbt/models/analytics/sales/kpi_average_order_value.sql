{{ config(materialized='view', schema='data_analytics') }}


WITH order_details AS (
    SELECT
        order_id,
        unit_price,
        quantity
    FROM {{ ref('stg_order_details') }}
),

orders AS (
    SELECT
        order_id
    FROM {{ ref('stg_orders') }}
),

calculated_values AS (
    SELECT
        SUM(od.unit_price * od.quantity) AS total_revenue,
        COUNT(DISTINCT o.order_id) AS total_orders
    FROM order_details od
    JOIN orders o ON od.order_id = o.order_id
)

SELECT
    total_revenue / NULLIF(total_orders, 0) AS average_order_value
FROM calculated_values
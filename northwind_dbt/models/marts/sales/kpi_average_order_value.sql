{{ config(materialized='table') }}


WITH revenue AS (
    SELECT SUM(total_price) AS total_revenue
    FROM {{ ref('stg_order_details') }}
),
orders AS (
    SELECT COUNT(order_id) AS total_orders
    FROM {{ ref('stg_orders') }}
)

SELECT
    revenue.total_revenue / orders.total_orders AS average_order_value
FROM revenue, orders;
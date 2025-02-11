{{ config(materialized='view', schema='data_analytics') }}

WITH order_details AS (
    SELECT
        order_id,
        quantity
    FROM {{ ref('stg_order_details') }}
)

SELECT
    SUM(quantity) / NULLIF(COUNT(DISTINCT order_id), 0) AS avg_items_per_order
FROM order_details

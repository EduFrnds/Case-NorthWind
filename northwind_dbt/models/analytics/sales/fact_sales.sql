{{
    config(
        materialized='view',
        schema='data_analytics',
        alias='fact_sales',
        tags=['fact'],
        unique_key='order_id'
    )
}}

WITH order_data AS (
    SELECT
        order_id,
        customer_id,
        employee_id,
        order_date,
        freight AS shipping_cost,
        shipper_id,
        shipper_name
    FROM {{ ref('stg_orders') }}
),

order_details AS (
    SELECT
        order_id,
        product_id,
        unit_price,
        quantity,
        discount,
        (unit_price * quantity * (1 - discount)) AS total_amount
    FROM {{ ref('stg_order_details') }}
)

SELECT
    o.order_id,
    o.customer_id,
    o.employee_id,
    o.order_date,
    o.shipping_cost,
    o.shipper_id,
    o.shipper_name,
    od.product_id,
    od.unit_price,
    od.quantity,
    od.discount,
    od.total_amount
FROM order_data o
LEFT JOIN order_details od
ON o.order_id = od.order_id

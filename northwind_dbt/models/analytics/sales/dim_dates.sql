{{
    config(
        materialized='table',
        schema='data_analytics',
        alias='dim_dates',
        tags=['dimension'],
        unique_key='date_id'
    )
}}

WITH dates AS (
    SELECT DISTINCT
        order_date::DATE AS date_id,
        EXTRACT(YEAR FROM order_date::DATE) AS year,
        EXTRACT(MONTH FROM order_date::DATE) AS month,
        EXTRACT(DAY FROM order_date::DATE) AS day,
        TRIM(TO_CHAR(order_date::DATE, 'Day')) AS weekday
    FROM {{ ref('stg_orders') }}
)

SELECT
    d.date_id,
    d.year,
    d.month,
    d.day,
    d.weekday
FROM dates d

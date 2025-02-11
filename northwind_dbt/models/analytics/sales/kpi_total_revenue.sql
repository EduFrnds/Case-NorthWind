{{ config(materialized='view', schema='data_analytics') }}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_order_details') }}
),

aggregated AS (
    SELECT
        SUM(total_price) AS total_revenue
    FROM source
)

SELECT *
FROM aggregated

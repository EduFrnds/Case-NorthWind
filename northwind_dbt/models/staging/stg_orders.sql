{{ config( materialized='view', schema='data_staging') }}

WITH source AS (
    SELECT *
    FROM {{ source('data_raw', 'orders') }}
),

renamed AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        shipped_date,

        CASE
            WHEN shipped_date IS NOT NULL THEN 1
            ELSE 0
        END AS is_shipped
    FROM source
)

SELECT *
FROM renamed

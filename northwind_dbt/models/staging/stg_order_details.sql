{{ config( materialized='view', schema='data_staging') }}

WITH source AS (
    SELECT *
    FROM {{ source('data_raw', 'order_details') }}
),

renamed AS (
    SELECT
        order_id,
        product_id,
        unit_price,
        quantity,
        unit_price * quantity AS total_price
    FROM source
)

SELECT *
FROM renamed
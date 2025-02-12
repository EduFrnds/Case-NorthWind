{{ config( materialized='view', schema='data_staging') }}

WITH source AS (
    SELECT *
    FROM {{ source('data_raw', 'products') }}
),

renamed AS (
    SELECT
        product_id,
        product_name,
        category_id,
        discontinued,
        unit_price,
        units_in_stock,
        units_on_order
    FROM source
)

SELECT *
FROM renamed
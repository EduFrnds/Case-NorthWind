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
        discontinued
    FROM source
)

SELECT *
FROM renamed
{{ config( materialized='view', schema='data_staging') }}

WITH source AS (
    SELECT *
    FROM {{ source('data_raw', 'categories') }}
),

renamed AS (
    SELECT
        category_id,
        category_name
    FROM source
)

SELECT *
FROM renamed
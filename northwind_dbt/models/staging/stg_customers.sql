{{ config( materialized='view', schema='data_staging') }}

WITH source AS (
    SELECT *
    FROM {{ source('data_raw', 'customers') }}
),

renamed AS (
    SELECT
        customer_id,
        company_name,
        country
    FROM source
)

SELECT *
FROM renamed

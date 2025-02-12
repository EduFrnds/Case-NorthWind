{{ config(
    materialized='incremental',
    schema='data_staging'
) }}

WITH source AS (
    SELECT *
    FROM {{ source('data_raw', 'customers') }}

    {% if is_incremental() %}
        WHERE update_date > (SELECT MAX(update_date) FROM {{ this }})
    {% endif %}
),

renamed AS (
    SELECT
        customer_id,
        contact_name,
        company_name,
        country
    FROM source
)

SELECT *
FROM renamed

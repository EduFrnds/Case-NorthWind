{{
    config(
        materialized='view',
        schema='data_staging'
    )
}}

SELECT
    shipper_id,
    company_name,
    phone
FROM {{ source('data_raw', 'shippers') }}

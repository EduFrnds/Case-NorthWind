{{
    config(
        materialized='view',
        schema='data_analytics',
        alias='dim_shippers',
        tags=['dimension'],
        unique_key='shipper_id'
    )
}}

SELECT
    shipper_id,
    company_name AS shipper_name,
    phone AS shipper_phone
FROM {{ ref('stg_shippers') }}

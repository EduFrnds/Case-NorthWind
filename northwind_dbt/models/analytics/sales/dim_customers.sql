{{
    config(
        materialized='view',
        schema='data_analytics',
        alias='dim_customers',
        tags=['dimension'],
        unique_key='customer_id'
    )
}}

WITH customers AS (
    SELECT
        customer_id,
        company_name,
        contact_name,
        country

    FROM {{ ref('stg_customers') }}
)

SELECT
    c.customer_id,
    c.contact_name,
    c.company_name,
    c.country

FROM customers c

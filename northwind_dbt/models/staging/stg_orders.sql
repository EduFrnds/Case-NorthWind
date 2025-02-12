{{
    config(
        materialized='view',
        schema='data_staging'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('data_raw', 'orders') }}
),

shippers AS (
    SELECT
        shipper_id,
        company_name
    FROM {{ source('data_raw', 'shippers') }}
),

renamed AS (
    SELECT
        o.order_id,
        o.employee_id,
        o.customer_id,
        o.order_date,
        o.shipped_date,
        o.freight,
        o.ship_via AS shipper_id,  -- Adicionando a chave do shipper

        CASE
            WHEN o.shipped_date IS NOT NULL THEN 1
            ELSE 0
        END AS is_shipped,

        s.company_name AS shipper_name  -- Adicionando o nome da transportadora

    FROM source o
    LEFT JOIN shippers s
    ON o.ship_via = s.shipper_id  -- Realizando a junção
)

SELECT *
FROM renamed


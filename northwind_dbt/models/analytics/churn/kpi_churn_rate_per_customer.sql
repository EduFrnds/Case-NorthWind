{{ config(
    materialized='view',
    schema='data_analytics',
    alias='kpi_churn_rate_per_customer',
    tags=['kpi']
) }}

WITH last_purchase_per_customer AS (
    SELECT
        f.customer_id,
        MAX(DATE_TRUNC('month', f.order_date::DATE)) AS last_purchase_month
    FROM {{ ref('fact_sales') }} f
    GROUP BY 1
),

latest_month AS (
    SELECT MAX(last_purchase_month) AS latest_active_month
    FROM last_purchase_per_customer
),

churn_status AS (
    SELECT
        l.customer_id,
        l.last_purchase_month,
        lm.latest_active_month,
        CASE
            WHEN l.last_purchase_month = lm.latest_active_month THEN 0  -- Cliente ativo
            ELSE 1  -- Cliente churned
        END AS churn_flag
    FROM last_purchase_per_customer l
    CROSS JOIN latest_month lm
)

SELECT
    c.customer_id,
    c.last_purchase_month,
    c.latest_active_month,
    c.churn_flag  -- 1 = churned, 0 = ativo
FROM churn_status c
ORDER BY c.churn_flag DESC, c.last_purchase_month


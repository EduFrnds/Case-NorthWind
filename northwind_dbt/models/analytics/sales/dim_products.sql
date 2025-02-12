WITH products AS (
    SELECT
        product_id,
        product_name,
        category_id::TEXT AS category_id,  -- Convertendo para STRING
        unit_price,
        units_in_stock,
        units_on_order,
        discontinued
    FROM {{ ref('stg_products') }}
),

categories AS (
    SELECT
        category_id::TEXT AS category_id,  -- Convertendo para STRING
        category_name
    FROM {{ ref('stg_categories') }}
)

SELECT
    p.product_id,
    p.product_name,
    p.category_id,
    COALESCE(c.category_name, 'Unknown') AS category_name,
    p.units_in_stock,
    p.units_on_order,
    p.discontinued
FROM products p
LEFT JOIN categories c
ON p.category_id = c.category_id  -- Agora os tipos são iguais

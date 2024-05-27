WITH product AS (
    SELECT *
    FROM {{ ref('stg_product') }}
),

model AS (
    SELECT *
    FROM {{ ref('stg_product_model') }}
),

subcategory AS (
    SELECT *
    FROM {{ ref('stg_product_subcategory') }}
),

category AS (
    SELECT *
    FROM {{ ref('stg_product_category') }}
),

measure AS (
    SELECT *
    FROM {{ ref('stg_unit_measure') }}
),

joined AS (
    SELECT
        product.pk_product,
        product.nm_product AS product,
        product.product_number AS number,
        category.nm_product_category AS category,
        subcategory.nm_subcategory AS subcategory,
        model.nm_product_model AS model,
        weight.nm_unit_measure AS unit_measure,
        product.safe_stock_level,
        product.reorder_point,
        product.standard_cost,
        product.list_price,
    FROM product
    LEFT JOIN model ON product.fk_product_model = model.pk_product_model
    LEFT JOIN subcategory ON product.fk_product_subcategory = subcategory.pk_product_subcategory
    LEFT JOIN category ON subcategory.fk_product_category = category.pk_product_category
    LEFT JOIN measure AS "size" ON product.size_unit_measure_code = "size".pk_unit_measure
    LEFT JOIN measure AS weight ON product.weight_unit_measure_code = weight.pk_unit_measure
)

SELECT *
FROM joined
WITH product AS (

    SELECT 
        product_id,
        product_model_id,
        product_subcategory_id,
        product_name,
        product_number,
        product_line
    FROM {{ ref('stg_product') }}

),

model AS (

    SELECT
        product_model_id,
        product_model_name
    FROM {{ ref('stg_product_model') }}

),

subcategory AS (

    SELECT
        product_subcategory_id,
        product_category_id,
        product_subcategory_name
    FROM {{ ref('stg_product_subcategory') }}

),

category AS (
    
    SELECT
        product_category_id,
        product_category_name
    FROM {{ ref('stg_product_category') }}

),

joined AS (

    SELECT
        product.product_id,
        product.product_name,
        product.product_number,
        COALESCE(category.product_category_name, 'Unknown') AS product_category_name,
        COALESCE(subcategory.product_subcategory_name, 'Unknown') AS product_subcategory_name,
        COALESCE(model.product_model_name, 'Unknown') AS product_model_name
    FROM product
    LEFT JOIN model ON product.product_model_id = model.product_model_id
    LEFT JOIN subcategory ON product.product_subcategory_id = subcategory.product_subcategory_id
    LEFT JOIN category ON subcategory.product_category_id = category.product_category_id

)

SELECT *
FROM joined

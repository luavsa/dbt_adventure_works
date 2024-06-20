WITH product_category_source AS (

    SELECT
        CAST(productcategoryid AS varchar) AS product_category_id,
        CAST(name AS varchar) AS product_category_name
    FROM {{ source("adventure_works", "productcategory") }}
    
)

SELECT *
FROM product_category_source
WITH product_subcategory_source AS (

    SELECT
        CAST(productsubcategoryid AS int) AS product_subcategory_id,
        CAST(productcategoryid AS int) AS product_category_id,
        CAST(name AS varchar) AS product_subcategory_name
    FROM {{ source("adventure_works", "productsubcategory") }}

)

SELECT *
FROM product_subcategory_source
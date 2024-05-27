WITH
   product_category_source AS (
        SELECT
           CAST(productcategoryid AS varchar) AS pk_product_category,
           CAST(name AS varchar) AS nm_product_category
        FROM {{ source("adventure_works", "productcategory") }}
    )

SELECT *
FROM product_category_source
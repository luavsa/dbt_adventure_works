WITH
   product_subcategory_source AS (
        SELECT
            CAST(productsubcategoryid AS int) AS pk_product_subcategory,
            CAST(productcategoryid AS int) AS fk_product_category,
            CAST(name AS varchar) AS nm_subcategory
        FROM {{ source("adventure_works", "productsubcategory") }}
    )

SELECT *
FROM product_subcategory_source
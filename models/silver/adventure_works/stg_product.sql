WITH product_source AS (

    SELECT
        CAST(productid AS int) AS product_id,
        CAST(productmodelid AS int) AS product_model_id,
        CAST(productsubcategoryid AS int) AS product_subcategory_id,
        CAST(name AS varchar) AS product_name,
        CAST(productnumber AS varchar) AS product_number,
        CAST(productline AS varchar) AS product_line
    FROM {{ source("adventure_works", "product") }}
    
)

SELECT *
FROM product_source
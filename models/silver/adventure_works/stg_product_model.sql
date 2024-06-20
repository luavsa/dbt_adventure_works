WITH product_model_source AS (

    SELECT
        CAST(productmodelid AS int) AS product_model_id,
        CAST(name AS varchar) AS product_model_name
    FROM {{ source("adventure_works", "productmodel") }}
    
)

SELECT *
FROM product_model_source
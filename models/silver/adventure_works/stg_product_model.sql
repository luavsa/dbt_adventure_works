WITH
   product_model_source AS (
        SELECT
            CAST(productmodelid AS int) AS pk_product_model,
            CAST(name AS varchar) AS nm_product_model
        FROM {{ source("adventure_works", "productmodel") }}
    )

SELECT *
FROM product_model_source
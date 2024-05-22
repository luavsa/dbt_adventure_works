WITH special_offer_product_source AS (
        SELECT 
            CAST(specialofferid AS int) AS fk_special_offer,
            CAST(productid AS int) AS fk_product,
            CAST(rowguid AS string) AS special_offer_product_rowguid,
            CAST(modifieddate AS varchar) AS special_offer_product_modified_dt
        FROM {{ source('adventure_works', 'specialofferproduct') }}
    )

SELECT *
FROM special_offer_product_source
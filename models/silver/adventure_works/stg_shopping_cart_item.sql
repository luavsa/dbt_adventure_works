WITH
    shopping_cart_item_source AS (
        SELECT
            CAST(shoppingcartitemid AS int) AS pk_shop_cart_item,
            CAST(shoppingcartid AS int) AS fk_shop_cart,
            CAST(quantity AS int) AS shop_cart_item_qty,
            CAST(productid AS int) AS fk_product,
            CAST(datecreated AS varchar) AS shop_cart_item_created_dt,
            CAST(modifieddate AS varchar) AS shop_cart_item_modified_dt
        FROM {{ source("adventure_works", "shoppingcartitem") }}
    )

SELECT *
FROM shopping_cart_item_source
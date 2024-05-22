WITH sales_order_detail_source AS (
        SELECT 
            CAST(salesorderid AS int) AS fk_sales_order,
            CAST(salesorderdetailid AS int) AS pk_sales_order_detail,
            CAST(carriertrackingnumber AS varchar) AS carrier_tracking_number,
            CAST(orderqty AS int) AS order_quantity,
            CAST(productid AS int) AS fk_product,
            CAST(specialofferid AS int) AS fk_special_offer,
            CAST(unitprice AS float) AS unit_price,
            CAST(unitpricediscount AS float) AS unit_price_discount,
            CAST(rowguid AS string) AS sl_order_detail_rowguid,
            CAST(modifieddate AS varchar) AS sl_order_detail_modified_dt
        FROM {{ source('adventure_works', 'salesorderdetail') }}
    )

SELECT *
FROM sales_order_detail_source
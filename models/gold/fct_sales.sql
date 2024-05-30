WITH sales AS (
    SELECT *
    FROM {{ ref('stg_sales_order_header') }}
),

sales_detail AS (
    SELECT *
    FROM {{ ref('stg_sales_order_detail') }}
),

joined AS (
    SELECT
        sales.pk_sales_order AS "pk.order",
        sales_detail.pk_sales_order_detail,
        sales.account_number,
        sales.fk_customer,
        sales.order_subtotal,
        sales.order_tax_mt,
        sales.order_freight,
        sales.order_total_due,
        sales_detail.order_quantity,
        sales_detail.fk_product,
        sales_detail.unit_price,
        sales_detail.unit_price_discount
    FROM sales
    LEFT JOIN sales_detail ON sales.pk_sales_order = sales_detail.fk_sales_order
)

SELECT *
FROM joined
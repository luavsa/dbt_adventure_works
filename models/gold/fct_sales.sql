WITH sales AS (
    SELECT *
    FROM {{ ref('stg_sales_order_header') }}
),

sales_detail AS (
    SELECT *
    FROM {{ ref('stg_sales_order_detail') }}
),

sales_reason AS (
    SELECT *
    FROM {{ ref('stg_sales_order_header_sales_reason') }}
),

customer AS (
    SELECT *
    FROM {{ ref('stg_customer') }}
),

joined AS (
    SELECT
        sales.pk_sales_order AS pk_order,
        sales_detail.pk_sales_order_detail,
        sales.account_number,
        sales.fk_customer,
        customer.fk_store,
        sales.fk_sales_person AS fk_employee,
        sales.fk_territory,
        sales.fk_credit_card,
        sales.order_date,
        sales.due_date,
        sales.ship_date,
        CASE
            WHEN sales.status = 1 THEN 'In process'
            WHEN sales.status = 2 THEN 'Approved'
            WHEN sales.status = 3 THEN 'Backordered'
            WHEN sales.status = 4 THEN 'Rejected'
            WHEN sales.status = 5 THEN 'Shipped'
            WHEN sales.status = 6 THEN 'Cancelled'
            END AS status,
        sales_reason.fk_sales_reason_id AS fk_sales_reason,
        sales.order_subtotal,
        sales.order_tax_mt,
        sales.order_freight,
        sales.order_total_due,
        sales_detail.order_quantity,
        sales_detail.fk_product,
        sales_detail.unit_price,
        sales_detail.unit_price_discount,
        (sales_detail.order_quantity * (sales_detail.unit_price * (1 - sales_detail.unit_price_discount))) AS total_value
    FROM sales
    LEFT JOIN sales_detail ON sales.pk_sales_order = sales_detail.fk_sales_order
    LEFT JOIN sales_reason ON sales.pk_sales_order = sales_reason.fk_order_id
    LEFT JOIN customer ON sales.fk_customer = customer.pk_customer
)

SELECT *
FROM joined
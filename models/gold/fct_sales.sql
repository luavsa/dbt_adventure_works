    WITH sales AS (
    
    SELECT *
    FROM {{ ref('stg_sales_order_header') }}

),

sales_detail AS (

    SELECT *
    FROM {{ ref('stg_sales_order_detail') }}

),

sales_reason_header AS (

    SELECT *
    FROM {{ ref('stg_sales_order_header_sales_reason') }}

),

sales_reason AS (

    SELECT *
    FROM {{ ref('stg_sales_reason') }}

),

customer AS (

    SELECT *
    FROM {{ ref('stg_customer') }}

),

reason AS (

    SELECT 
        sales_reason_header.fk_order_id,
        sales_reason.sales_reason_type,
        1 AS is_flag
    FROM sales_reason_header
    JOIN sales_reason ON sales_reason_header.fk_sales_reason_id = sales_reason.pk_sales_reason
    
),

pivot_reason AS (

    SELECT
        fk_order_id,
        COALESCE("'Other'", 0) AS is_other,
        COALESCE("'Promotion'", 0) AS is_promotion,
        COALESCE("'Marketing'", 0) AS is_marketing
    FROM reason
    PIVOT (MAX(is_flag) FOR sales_reason_type IN ('Other', 'Promotion', 'Marketing'))

),

joined AS (
    SELECT
        sales.pk_sales_order AS pk_order,
        sales_detail.pk_sales_order_detail,
        sales.account_number,
        sales.fk_customer,
        sales.fk_sales_person AS fk_employee,
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
        CASE pivot_reason.is_other WHEN 1 THEN sales.pk_sales_order END AS is_other,
        CASE pivot_reason.is_promotion WHEN 1 THEN sales.pk_sales_order END AS is_promotion,
        CASE pivot_reason.is_marketing WHEN 1 THEN sales.pk_sales_order END AS is_marketing,
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
    LEFT JOIN pivot_reason ON sales.pk_sales_order = pivot_reason.fk_order_id
    LEFT JOIN customer ON sales.fk_customer = customer.customer_id
)

SELECT *
FROM joined
WITH sales_order_header_sales_reason_source AS (
        SELECT 
            CAST(salesorderid AS int) AS fk_order_id,
            CAST(salesreasonid AS int) AS fk_sales_reason_id,
            CAST(modifieddate AS varchar) AS slorder_slreason_modified_dt
        FROM {{ source('adventure_works', 'salesorderheadersalesreason') }}
    )

SELECT *
FROM sales_order_header_sales_reason_source
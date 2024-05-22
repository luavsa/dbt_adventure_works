WITH sales_reason_source AS (
        SELECT 
            CAST(salesreasonid AS varchar) AS pk_sales_reason,
            CAST(name AS varchar) AS nm_sales_reason,
            CAST(reasontype AS varchar) AS sales_reason_type,
            CAST(modifieddate AS varchar) AS sl_reason_modified_dt
        FROM {{ source('adventure_works', 'salesreason') }}
    )

SELECT *
FROM sales_reason_source
WITH reason AS (
    SELECT *
    FROM {{ ref('stg_sales_reason') }}
)

SELECT
    pk_sales_reason,
    nm_sales_reason AS sales_reason,
    sales_reason_type AS type
FROM reason

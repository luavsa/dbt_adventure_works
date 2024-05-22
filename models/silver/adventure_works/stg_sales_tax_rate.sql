WITH
    sales_tax_rate_source AS (
        SELECT
            CAST(salestaxrateid AS int) AS pk_sales_tax_rate,
            CAST(stateprovinceid AS int) AS fk_state_province,
            CAST(taxtype AS int) AS tax_type,
            CAST(taxrate AS float) AS tax_rate,
            CAST(name AS varchar) AS nm_tax,
            CAST(rowguid AS varchar) AS tax_rate_rowguid,
            CAST(modifieddate AS varchar) AS tax_rate_modified_dt
        FROM {{ source("adventure_works", "salestaxrate") }}
    )

SELECT *
FROM sales_tax_rate_source
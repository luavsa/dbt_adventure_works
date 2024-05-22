WITH store_source AS (
        SELECT 
            CAST(businessentityid AS int) AS pk_business_entity,
            CAST(name AS string) AS nm_store,
            CAST(salespersonid AS int) AS fk_sales_person,
            CAST(demographics AS string) AS store_demographics,
            CAST(rowguid AS string) AS store_rowguid,
            CAST(modifieddate AS string) AS store_modifieddt
        FROM {{ source('adventure_works', 'store') }}
    )

SELECT *
FROM store_source
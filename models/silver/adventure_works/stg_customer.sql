WITH customer_source AS (
        SELECT 
            CAST(customerid AS int) AS pk_customer,
            CAST(personid AS int) AS fk_person,
            CAST(storeid AS int) AS fk_store,
            CAST(territoryid AS int) AS fk_territory,
            CAST(rowguid AS string) AS customer_rowguid,
            CAST(modifieddate AS date) AS customer_modifieddt
        FROM {{ source('adventure_works', 'customer') }}
    )

SELECT *
FROM customer_source
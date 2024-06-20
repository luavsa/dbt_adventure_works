WITH customer_source AS (

    SELECT 
        CAST(customerid AS int) AS customer_id,
        CAST(personid AS int) AS person_id,
        CAST(storeid AS int) AS store_id
    FROM {{ source('adventure_works', 'customer') }}
    
)

SELECT *
FROM customer_source
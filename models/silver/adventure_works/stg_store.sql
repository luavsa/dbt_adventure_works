WITH store_source AS (

    SELECT 
        CAST(businessentityid AS int) AS business_entity_id,
        CAST(salespersonid AS int) AS sales_person_id,
        CAST(name AS string) AS store_name        
    FROM {{ source('adventure_works', 'store') }}
    
)

SELECT *
FROM store_source
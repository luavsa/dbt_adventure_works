WITH business_entity_source AS (

    SELECT CAST(businessentityid AS int) AS business_entity_id
    FROM {{ source("adventure_works", "businessentity") }}
    
)

SELECT *
FROM business_entity_source

WITH business_entity_address_source AS (

    SELECT
        CAST(businessentityid AS int) AS business_entity_id,
        CAST(COALESCE(addressid, 0) AS int) AS address_id,
        CAST(addresstypeid AS int) AS address_type_id
    FROM {{ source("adventure_works", "businessentityaddress") }}

)

SELECT *
FROM business_entity_address_source
WITH
    business_entity_address_source AS (
        SELECT
            CAST(businessentityid AS int) AS fk_business_entity,
            CAST(addressid AS int) AS fk_address,
            CAST(addresstypeid AS int) AS fk_address_type,
            CAST(rowguid AS varchar) AS bus_ety_add_rowguid,
            CAST(modifieddate AS varchar) AS bus_ety_add_modified_dt
        FROM {{ source("adventure_works", "businessentityaddress") }}
    )

SELECT *
FROM business_entity_address_source

WITH
    business_entity_source AS (
        SELECT
            CAST(businessentityid AS int) AS pk_business_entity,
            CAST(rowguid AS varchar) AS bus_ety_rowguid,
            CAST(modifieddate AS varchar) AS bus_ety_modified_dt
        FROM {{ source("adventure_works", "businessentity") }}
    )

SELECT *
FROM business_entity_source

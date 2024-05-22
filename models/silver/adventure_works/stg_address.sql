WITH
    address_source AS (
        SELECT
            CAST(addressid AS int) AS pk_address,
            CAST(addressline1 AS varchar) || ' ' || CAST(addressline2 AS varchar) AS address,
            CAST(city AS varchar) AS city,
            CAST(stateprovinceid AS int) AS fk_state_province,
            CAST(postalcode AS varchar) AS postal_code,
            CAST(spatiallocation AS varchar) AS spacial_location,
            CAST(rowguid AS varchar) AS add_rowguid,
            CAST(modifieddate AS varchar) AS add_modified_dt
        FROM {{ source("adventure_works", "address") }}
    )

SELECT *
FROM address_source

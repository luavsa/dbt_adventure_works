WITH
    state_province_source AS (
        SELECT
            CAST(stateprovinceid AS int) AS pk_state_province,
            CAST(stateprovincecode AS varchar) AS state_province_code,
            CAST(countryregioncode AS varchar) AS country_region_code,
            CAST(isonlystateprovinceflag AS boolean) AS is_only_state_province_flag,
            CAST(name AS varchar) AS nm_state_province,
            CAST(territoryid AS int) AS fk_territory,
            CAST(rowguid AS varchar) AS state_province_rowguid,
            CAST(modifieddate AS varchar) AS state_province_modified_dt
        FROM {{ source("adventure_works", "stateprovince") }}
    )

SELECT *
FROM state_province_source
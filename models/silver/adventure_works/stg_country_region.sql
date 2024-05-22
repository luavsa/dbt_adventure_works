WITH
    country_region_source AS (
        SELECT
            CAST(countryregioncode AS varchar) AS pk_country,
            CAST(name AS varchar) AS nm_country,
            CAST(modifieddate AS varchar) AS country_modified_dt
        FROM {{ source("adventure_works", "countryregion") }}
    )

SELECT *
FROM country_region_source
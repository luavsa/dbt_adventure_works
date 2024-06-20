WITH country_region_source AS (

    SELECT
        CAST(countryregioncode AS varchar) AS country_id,
        CAST(name AS varchar) AS country_name
    FROM {{ source("adventure_works", "countryregion") }}

)

SELECT *
FROM country_region_source
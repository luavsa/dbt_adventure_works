WITH state_province_source AS (

    SELECT
        CAST(stateprovinceid AS int) AS state_province_id,
        CAST(countryregioncode AS varchar) AS country_id,
        CAST(stateprovincecode AS varchar) AS state_province_code,
        CAST(name AS varchar) AS state_province_name
    FROM {{ source("adventure_works", "stateprovince") }}

)

SELECT *
FROM state_province_source
WITH address_source AS (

    SELECT
        CAST(addressid AS int) AS address_id,
        CAST(stateprovinceid AS int) AS state_province_id,
        CAST(city AS varchar) AS city_name
    FROM {{ source("adventure_works", "address") }}

)

SELECT *
FROM address_source

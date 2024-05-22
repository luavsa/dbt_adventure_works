WITH
    country_region_currency_source AS (
        SELECT
            CAST(countryregioncode AS varchar) AS pk_country_currency,
            CAST(currencycode AS varchar) AS fk_currency,
            CAST(modifieddate AS varchar) AS country_currency_modified_dt
        FROM {{ source("adventure_works", "countryregioncurrency") }}
    )

SELECT *
FROM country_region_currency_source
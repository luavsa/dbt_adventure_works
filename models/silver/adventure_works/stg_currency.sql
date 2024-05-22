WITH currency_source AS (
        SELECT 
            CAST(currencycode AS varchar) AS pk_currency_code,
            CAST(name AS varchar) AS nm_currency,
            CAST(modifieddate AS varchar) AS currency_modified_dt
        FROM {{ source('adventure_works', 'currency') }}
    )

SELECT *
FROM currency_source
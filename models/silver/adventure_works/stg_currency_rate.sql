WITH currency_rate_source AS (
        SELECT 
            CAST(currencyrateid AS int) AS pk_currency_rate_id,
            CAST(currencyratedate AS varchar) AS currency_rate_dt,
            CAST(fromcurrencycode AS varchar) AS from_currency_code,
            CAST(tocurrencycode AS varchar) AS to_currency_code,
            CAST(averagerate AS float) AS average_rate,
            CAST(endofdayrate AS float) AS end_of_day_rate,
            CAST(modifieddate AS varchar) AS currency_rate_modified_dt,
        FROM {{ source('adventure_works', 'currencyrate') }}
    )

SELECT *
FROM currency_rate_source
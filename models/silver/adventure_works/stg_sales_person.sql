WITH sales_person_source AS (
        SELECT 
            CAST(businessentityid AS int) AS fk_business_entity,
            CAST(territoryid AS int) AS fk_territory,
            CAST(salesquota AS int) AS sales_quota,
            CAST(bonus AS int) AS bonus,
            CAST(commissionpct AS float) AS comission_pct,
            CAST(salesytd AS float) AS sales_ytd,
            CAST(saleslastyear AS float) AS sales_last_year,
            CAST(rowguid AS string) AS sales_person_rowguid,
            CAST(modifieddate AS string) AS salesps_modifieddt
        FROM {{ source('adventure_works', 'salesperson') }}
    )

SELECT *
FROM sales_person_source
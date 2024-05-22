WITH sales_territory_history_source AS (
        SELECT 
            CAST(businessentityid AS int) AS fk_business_entity, 
            CAST(territoryid AS int) AS fk_territory, 
            CAST(startdate AS varchar) AS startdt_salestt_history, 
            CAST(enddate AS varchar) AS enddt_salestt_history, 
            CAST(rowguid AS string) AS salestt_history_rowguid, 
            CAST(modifieddate AS varchar) AS salestt_history_modifiedtt
        FROM {{ source('adventure_works', 'salesterritoryhistory') }}
    )

SELECT *
FROM sales_territory_history_source
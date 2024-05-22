WITH
   ship_method_source AS (
        SELECT
            CAST(shipmethodid AS int) AS pk_ship_method,
            CAST(name AS varchar) AS nm_ship_method,
            CAST(shipbase AS float) AS ship_base,
            CAST(shiprate AS float) AS ship_rate,
            CAST(rowguid AS varchar) AS ship_rowguid,
            CAST(modifieddate AS varchar) AS ship_modified_dt
        FROM {{ source("adventure_works", "shipmethod") }}
    )

SELECT *
FROM ship_method_source
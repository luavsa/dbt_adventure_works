WITH
   unit_measure_source AS (
        SELECT
            CAST(unitmeasurecode AS varchar) AS pk_unit_measure,
            CAST(name AS varchar) AS nm_unit_measure
        FROM {{ source("adventure_works", "unitmeasure") }}
    )

SELECT *
FROM unit_measure_source
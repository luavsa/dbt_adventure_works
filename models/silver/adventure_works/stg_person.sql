WITH person_source AS (
        
    SELECT
        CAST(businessentityid AS int) AS person_id,
        CAST(persontype AS varchar) AS person_type,
        CAST(COALESCE(firstname, '') AS varchar) AS first_name,
        CAST(COALESCE(middlename, '') AS varchar) AS middle_name,
        CAST(COALESCE(lastname, '') AS varchar) AS last_name
    FROM {{ source("adventure_works", "person") }}

),

person AS (

    SELECT 
        person_id,
        CASE person_type
            WHEN 'SC' THEN 'Store Contact'
            WHEN 'IN' THEN 'Individual Customer'
            WHEN 'SP' THEN 'Sales Person'
            WHEN 'EM' THEN 'Employee'
            WHEN 'VC' THEN 'Vendor Contact'
            WHEN 'GC' THEN 'General Contact'
        END AS person_type,
        TRIM(CONCAT(first_name, ' ', middle_name, ' ', last_name)) AS person_name
    FROM person_source

)

SELECT *
FROM person

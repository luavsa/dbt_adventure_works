WITH
    person_source AS (
        SELECT
            CAST(businessentityid AS int) AS pk_person,
            CAST(persontype AS varchar) AS person_type,
            CAST(namestyle AS boolean) AS name_style,
            CAST(title AS varchar) AS person_title,
            CAST(COALESCE(firstname, '') AS varchar) || ' ' || CAST(COALESCE(middlename, '') AS varchar) || ' ' || CAST(COALESCE(lastname, '') AS varchar) AS person_name,
            CAST(emailpromotion AS string) AS email_promotion,
            CAST(additionalcontactinfo AS string) AS person_additional_contact_info,
            CAST(demographics AS string) AS person_demographics,
            CAST(rowguid AS string) AS person_rowguid,
            CAST(modifieddate AS string) AS person_modifieddt
        FROM {{ source("adventure_works", "person") }}
    )

SELECT *
FROM person_source

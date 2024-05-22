WITH
    person_source AS (
        SELECT
            CAST(businessentityid AS int) AS pk_business_entity,
            CAST(persontype AS varchar) AS person_type,
            CAST(namestyle AS boolean) AS name_style,
            CAST(title AS varchar) AS person_title,
            CAST(firstname AS varchar) AS person_first_name,
            CAST(middlename AS varchar) AS person_middle_name,
            CAST(lastname AS varchar) AS person_last_name,
            CAST(suffix AS varchar) AS person_suffix,
            CAST(emailpromotion AS string) AS email_promotion,
            CAST(additionalcontactinfo AS string) AS person_additional_contact_info,
            CAST(demographics AS string) AS person_demographics,
            CAST(rowguid AS string) AS person_rowguid,
            CAST(modifieddate AS string) AS person_modifieddt
        from {{ source("adventure_works", "person") }}
    )

SELECT *
FROM person_source

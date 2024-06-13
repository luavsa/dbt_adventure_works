WITH person_credit_card_source AS (
        SELECT 
            CAST(businessentityid AS int) AS pk_business_entity,
            CAST(creditcardid AS int) AS fk_credit_card,
            CAST(modifieddate AS varchar) AS person_card_modified_dt
        FROM {{ source('adventure_works', 'personcreditcard') }}
    )

SELECT *
FROM person_credit_card_source
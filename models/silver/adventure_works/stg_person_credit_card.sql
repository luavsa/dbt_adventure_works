WITH person_credit_card_source AS (
        SELECT 
            CAST(businessentityid AS int) AS fk_business_entity_id,
            CAST(creditcardid AS int) AS fk_credit_card_id,
            CAST(modifieddate AS varchar) AS person_card_modified_dt
        FROM {{ source('adventure_works', 'personcreditcard') }}
    )

SELECT *
FROM person_credit_card_source
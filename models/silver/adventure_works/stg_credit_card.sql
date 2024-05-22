WITH credit_card_source AS (
        SELECT 
          CAST(creditcardid AS int) AS pk_credit_card_id, 
          CAST(cardtype AS varchar) AS credit_card_type, 
          CAST(cardnumber AS int) AS credit_card_number, 
          CAST(expmonth AS int) AS card_exp_month, 
          CAST(expyear AS int) AS card_exp_year, 
          CAST(modifieddate AS varchar) AS card_modified_dt
        FROM {{ source('adventure_works', 'creditcard') }}
    )

SELECT *
FROM credit_card_source
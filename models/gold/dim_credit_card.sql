WITH card AS (
    SELECT *
    FROM {{ ref('stg_credit_card') }}
),

joined AS (
        SELECT
            card.pk_credit_card,
            card.credit_card_type
    FROM card
    )

SELECT *
FROM joined

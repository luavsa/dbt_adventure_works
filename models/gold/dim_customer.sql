WITH customer AS (
    SELECT *
    FROM {{ ref('stg_customer') }}
),

person AS (
    SELECT *
    FROM {{ ref('stg_person') }}
),

joined AS (
    SELECT
    customer.pk_customer,
    person.person_first_name || ' ' || person.person_middle_name || ' ' || person.person_last_name AS customer
    FROM customer
    LEFT JOIN person ON customer.fk_person = person.pk_business_entity
)

SELECT *
FROM joined


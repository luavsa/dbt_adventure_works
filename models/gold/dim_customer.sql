WITH customer AS (
    SELECT *
    FROM {{ ref('stg_customer') }}
),

person AS (
    SELECT *
    FROM {{ ref('stg_person') }}
),

address AS (
    SELECT *
    FROM {{ ref('stg_customer_address') }}
),

joined AS (
    SELECT
    customer.pk_customer,
    person.person_first_name || ' ' || person.person_middle_name || ' ' || person.person_last_name AS customer,
    address.address,
    address.city,
    address.state_province,
    address.state_province_acronym,
    address.country,
    address.country_acronym,
    address.postal_code
    FROM customer
    LEFT JOIN person ON customer.fk_person = person.pk_business_entity
    LEFT JOIN address ON customer.pk_customer = address.pk_customer
)

SELECT *
FROM joined


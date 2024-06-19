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
    SELECT DISTINCT
    customer.pk_customer,
    person.person_name AS customer,
    address.address,
    address.city,
    address.state_province,
    address.country,
    address.postal_code,
    FROM customer
    LEFT JOIN person ON customer.fk_person = person.pk_person
    LEFT JOIN address ON customer.pk_customer = address.pk_customer
)

SELECT *
FROM joined


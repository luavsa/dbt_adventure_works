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

store AS (
    SELECT *
    FROM {{ ref('stg_store') }}
),

pcard AS (
    SELECT *
    FROM {{ ref('stg_person_credit_card') }}
),

joined AS (
    SELECT DISTINCT
    customer.pk_customer,
    person.person_name AS customer,
    store.pk_business_entity AS pk_store,
    store.nm_store AS store,
    address.address,
    address.city,
    address.state_province,
    address.country,
    address.postal_code,
    pcard.fk_credit_card
    FROM customer
    LEFT JOIN person ON customer.fk_person = person.pk_person
    LEFT JOIN address ON customer.pk_customer = address.pk_customer
    LEFT JOIN store ON customer.fk_store = store.pk_business_entity
    LEFT JOIN pcard ON person.pk_person = pcard.pk_business_entity
)

SELECT *
FROM joined


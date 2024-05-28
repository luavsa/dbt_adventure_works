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

joined AS (
    SELECT
    customer.pk_customer,
    person.person_first_name || ' ' || person.person_middle_name || ' ' || person.person_last_name AS customer,
    store.pk_business_entity AS pk_store,
    store.nm_store AS store,
    address.address,
    address.city,
    address.state_province,
    address.country,
    address.postal_code
    FROM customer
    LEFT JOIN person ON customer.fk_person = person.pk_business_entity
    LEFT JOIN address ON customer.pk_customer = address.pk_customer
    LEFT JOIN store ON customer.fk_store = store.pk_business_entity
)

SELECT *
FROM joined


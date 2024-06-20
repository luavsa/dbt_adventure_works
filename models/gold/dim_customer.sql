WITH customer AS (

    SELECT
        customer_id,
        person_id,
        store_id
    FROM {{ ref('stg_customer') }}

),

person AS (

    SELECT
        person_id,
        person_type,
        person_name
    FROM {{ ref('stg_person') }}

),

store AS (

    SELECT 
        business_entity_id,
        sales_person_id,
        store_name
    FROM {{ ref('stg_store') }}

),

address AS (

    SELECT
        business_entity_id,
        city_name,
        state_province_name,
        country_name
    FROM {{ ref('entity_address') }}

),

joined AS (

    SELECT
    customer.customer_id,
    CASE 
        WHEN customer.store_id IS NOT NULL THEN 'Store'
        WHEN customer.person_id IS NOT NULL THEN 'Individual Customer'        
    END AS customer_type,
    COALESCE(store.store_name, person.person_name) AS customer_name,
    COALESCE(person_address.city_name, store_address.city_name) AS city_name,
    COALESCE(person_address.state_province_name, store_address.state_province_name) AS state_province_name,
    COALESCE(person_address.country_name, store_address.country_name) AS country_name
    FROM customer
    LEFT JOIN person ON customer.person_id = person.person_id
    LEFT JOIN address AS person_address ON person.person_id = person_address.business_entity_id
    LEFT JOIN store ON customer.store_id = store.business_entity_id
    LEFT JOIN address AS store_address ON store.business_entity_id = store_address.business_entity_id

)

SELECT *
FROM joined


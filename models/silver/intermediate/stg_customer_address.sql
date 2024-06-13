WITH customer AS (
    SELECT *
    FROM {{ ref('stg_customer') }}
),

business_entity_address AS (
    SELECT *
    FROM {{ ref('stg_business_entity_address') }}
    WHERE fk_address_type = 2
),

address AS (
    SELECT *
    FROM {{ ref('stg_address') }}
),

state AS (
    SELECT *
    FROM {{ ref('stg_state_province') }}
),

country AS (
    SELECT *
    FROM {{ ref('stg_country_region') }}
),

joined AS (
    SELECT
        customer.pk_customer,
        address.address,
        address.city,
        state.nm_state_province AS state_province,
        state.state_province_code AS state_province_acronym,
        country.nm_country AS country,
        country.pk_country AS country_acronym,
        address.postal_code
    FROM customer
    LEFT JOIN business_entity_address ON customer.fk_person = business_entity_address.fk_business_entity
    LEFT JOIN address ON business_entity_address.fk_address = address.pk_address
    LEFT JOIN state ON address.fk_state_province = state.pk_state_province
    LEFT JOIN country ON state.country_region_code = country.pk_country
)

SELECT *
FROM joined
WITH business_entity_address AS (

    SELECT
        business_entity_id,
        "1" AS billing_address_id,
        "2" AS home_address_id,
        "3" AS office_address_id,
        "4" AS primary_address_id,
        "5" AS shipping_address_id,
        "6" AS archive_address_id
    FROM {{ ref('stg_business_entity_address') }}
    PIVOT (MAX(address_id) FOR address_type_id IN (1, 2, 3, 4, 5, 6))
    
),


address AS (

    SELECT
        address_id,
        state_province_id,
        city_name
    FROM {{ ref('stg_address') }}

),

state AS (

    SELECT
        country_id,
        state_province_id,
        state_province_name
    FROM {{ ref('stg_state_province') }}

),

country AS (

    SELECT
        country_id,
        country_name
    FROM {{ ref('stg_country_region') }}

),

full_address AS (

    SELECT
        address.address_id,
        address.city_name,
        state.state_province_name,
        country.country_name
    FROM address 
    LEFT JOIN state ON address.state_province_id = state.state_province_id
    LEFT JOIN country ON state.country_id = country.country_id

),

joined AS (

    SELECT DISTINCT
        business_entity_address.business_entity_id,
        COALESCE(home.city_name, office.city_name, shipping.city_name) AS city_name,
        COALESCE(home.state_province_name, office.state_province_name, shipping.state_province_name) AS state_province_name,
        COALESCE(home.country_name, office.country_name, shipping.country_name) AS country_name
    FROM business_entity_address
    LEFT JOIN full_address AS home ON business_entity_address.home_address_id = home.address_id
    LEFT JOIN full_address AS office ON business_entity_address.office_address_id = office.address_id
    LEFT JOIN full_address AS shipping ON business_entity_address.shipping_address_id = shipping.address_id

)

SELECT *
FROM joined

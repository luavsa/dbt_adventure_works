WITH store AS (
    SELECT *
    FROM {{ ref('stg_store') }}
),

joined AS (
    SELECT DISTINCT
    store.pk_business_entity AS pk_store,
    store.nm_store AS store,
    FROM store 
)

SELECT *
FROM joined


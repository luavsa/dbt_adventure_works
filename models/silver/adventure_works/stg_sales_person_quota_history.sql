WITH
   sales_person_quota_history_source AS (
        SELECT
            CAST(businessentityid AS int) AS fk_business_entity,
            CAST(quotadate AS varchar) AS quota_dt,
            CAST(salesquota AS int) AS sales_quota,
            CAST(rowguid AS varchar) AS quota_rowguid,
            CAST(modifieddate AS varchar) AS quota_modified_dt,
        FROM {{ source("adventure_works", "salespersonquotahistory") }}
    )

SELECT *
FROM sales_person_quota_history_source
WITH special_offer_source AS (
        SELECT 
            CAST(specialofferid AS int) AS pk_special_offer,
            CAST(description AS varchar) AS offer_description,
            CAST(discountpct AS float) AS offer_discount,
            CAST(type AS varchar) AS offer_type,
            CAST(category AS varchar) AS offer_category,
            CAST(startdate AS varchar) AS offer_start_dt,
            CAST(enddate AS varchar) AS offer_end_dt,
            CAST(minqty AS int) AS offer_min_qty,
            CAST(maxqty AS int) AS offer_max_qty
        FROM {{ source('adventure_works', 'specialoffer') }}
    )

SELECT *
FROM special_offer_source
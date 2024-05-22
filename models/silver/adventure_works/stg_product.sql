WITH
   product_source AS (
        SELECT
            CAST(productid AS int) AS pk_product,
            CAST(name AS varchar) AS nm_product,
            CAST(productnumber AS varchar) AS product_number,
            CAST(makeflag AS boolean) AS make_flag,
            CAST(finishedgoodsflag AS boolean) AS finished_goods_flag,
            CAST(color AS varchar) AS color,
            CAST(safetystocklevel AS int) AS safe_stock_level,
            CAST(reorderpoint AS int) AS reorder_point,
            CAST(standardcost AS float) AS standard_cost,
            CAST(listprice AS float) AS list_price,
            CAST(size AS varchar) AS size,
            CAST(sizeunitmeasurecode AS varchar) AS size_unit_measure_code,
            CAST(weightunitmeasurecode AS varchar) AS weight_unit_measure_code,
            CAST(weight AS float) AS weight,
            CAST(daystomanufacture AS int) AS days_to_manufacture,
            CAST(productline AS varchar) AS product_line,
            CAST(class AS varchar) AS class,
            CAST(style AS varchar) AS style,
            CAST(productsubcategoryid AS int) AS fk_product_subcategory,
            CAST(productmodelid AS int) AS fk_product_model,
            CAST(sellstartdate AS varchar) AS sell_start_dt,
            CAST(sellenddate AS varchar) AS sell_end_dt,
            CAST(discontinueddate AS varchar) AS discontinued_dt,
            CAST(rowguid AS varchar) AS product_rowguid,
            CAST(modifieddate AS varchar) AS product_modified_dt
        FROM {{ source("adventure_works", "product") }}
    )

SELECT *
FROM product_source
WITH sales_order_header_source AS (
        SELECT 
            CAST(salespersonid AS int) AS pk_sales_orderid,
            CAST(territoryid AS int) AS fk_territory_id,
            CAST(billtoaddressid AS int) AS fk_bill_address_id,
            CAST(shiptoaddressid AS int) AS fk_ship_address_id,
            CAST(shipmethodid AS int) AS fk_ship_method_id,
            CAST(creditcardid AS int) AS fk_credit_card_id,
            CAST(creditcardapprovalcode AS varchar) AS credit_card_approval_code,
            CAST(currencyrateid AS varchar) AS fk_currency_rate_id,
            CAST(subtotal AS float) AS order_subtotal,
            CAST(taxamt AS float) AS order_tax_mt,
            CAST(freight AS float) AS order_freight,
            CAST(totaldue AS float) AS order_total_due,
            CAST(comment AS string) AS order_comment,
            CAST(rowguid AS string) AS order_rowguid,
            CAST(modifieddate AS varchar) AS order_modified_dt
        FROM {{ source('adventure_works', 'salesorderheader') }}
    )

SELECT *
FROM sales_order_header_source
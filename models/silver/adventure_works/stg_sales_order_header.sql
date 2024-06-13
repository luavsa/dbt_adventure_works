WITH sales_order_header_source AS (
        SELECT 
            CAST(salesorderid AS int) AS pk_sales_order,
            CAST(salespersonid AS int) AS fk_sales_person,
            CAST(territoryid AS int) AS fk_territory,
            CAST(revisionnumber AS int) AS revision_number,
            CAST(orderdate AS date) AS order_date,
            CAST(duedate AS date) AS due_date,
            CAST(shipdate AS date) AS ship_date,
            CAST(status AS int) AS status,
            CAST(onlineorderflag AS boolean) AS online_order_flag,
            CAST(purchaseordernumber AS varchar) AS purchase_order_number,
            CAST(accountnumber AS varchar) AS account_number,
            CAST(customerid AS int) AS fk_customer,
            CAST(billtoaddressid AS int) AS fk_bill_address,
            CAST(shiptoaddressid AS int) AS fk_ship_address,
            CAST(shipmethodid AS int) AS fk_ship_method,
            CAST(creditcardid AS int) AS fk_credit_card,
            CAST(creditcardapprovalcode AS varchar) AS credit_card_approval_code,
            CAST(currencyrateid AS varchar) AS fk_currency_rate,
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
{{ config(
    alias = target.database + '_shopify_sales_by_product'
) }}


WITH shopify_data AS (

    -- SALES ROWS
    SELECT

        order_date,
        order_line_id,
        product_id,
        title,
        name,
        sku,
  
        -- Gross Revenue
        SUM(COALESCE(price * quantity,0)) AS order_gross_revenue,
        SUM(COALESCE(price * refund_quantity,0)) AS refund_gross_revenue,

        -- Quantity
        SUM(COALESCE(quantity,0)) AS order_quantity,
        SUM(COALESCE(refund_quantity,0)) AS refund_quantity,
  

        -- Subtotal Discounts
        SUM(COALESCE(discount_amount,0)) AS subtotal_discount,

        -- Subtotal Refunds
        SUM(COALESCE(refund_subtotal,0)) AS subtotal_refund,

        -- Net Quantity
        SUM(COALESCE(quantity) - COALESCE(refund_quantity)) as net_quantity,

        -- Net Subtotal
        SUM(COALESCE(price * quantity) - COALESCE(refund_subtotal)) as net_subtotal


    FROM {{ ref('shopify_line_items') }}
  group by 1,2,3,4,5,6


SELECT *
FROM shopify_data



{{ config (
    alias = target.database + '_shopify_daily_refunds'
)}}

WITH 
    refunds AS 
    (SELECT 
        date as date,
        refund_id,
        order_id, 
        COALESCE(subtotal_order_refund,0)+COALESCE(subtotal_line_refund,0) as subtotal_refund,
        shipping_refund,
        tax_refund
    FROM {{ ref('shopify_refunds') }}
    GROUP BY date, order_id
    ),

    order_customer AS 
    (SELECT order_id, customer_id
    FROM {{ ref('shopify_orders') }}
    )

SELECT *,
    {{ get_date_parts('date') }}
FROM refunds
LEFT JOIN order_customer USING(order_id)

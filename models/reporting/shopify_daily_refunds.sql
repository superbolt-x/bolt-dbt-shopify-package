

{{ config (
    alias = target.database + '_shopify_daily_refunds'
)}}

WITH 
    refunds AS 
    (SELECT 
        date::date as date,
        refund_id,
        order_id, 
        case
            when subtotal_order_refund > 0 and subtotal_line_refund+tax_refund+shipping_refund=0 then subtotal_order_refund
            when subtotal_line_refund > 0 and subtotal_order_refund > 0 then -tax_refund
            when shipping_refund>0 and subtotal_order_refund>0 and subtotal_line_refund+tax_refund=0 then -shipping_refund
            when subtotal_line_refund>0 and subtotal_order_refund=0 then subtotal_line_refund
            else 0
        end as subtotal_refund,
        shipping_refund,
        tax_refund
    FROM {{ ref('shopify_refunds') }}
    ),

    order_customer AS 
    (SELECT order_id, customer_id, cancelled_at
    FROM {{ ref('shopify_orders') }}
    )

SELECT *,
    {{ get_date_parts('date') }}
FROM refunds
LEFT JOIN order_customer USING(order_id)
WHERE cancelled_at is null

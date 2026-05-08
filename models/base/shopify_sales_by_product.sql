{{ config(
    alias = target.database + '_shopify_sales_by_product'
) }}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH sales_data AS (

    SELECT
        order_id,
        order_line_id,
        product_id,
        variant_id,

        title,
        variant_title,
        name,

        order_date AS transaction_date,

        day,
        week,
        month,
        quarter,
        year,

        sku,
        fulfillment_status,
        gift_card,

        SUM(COALESCE(price * quantity, 0)) AS order_gross_revenue,
        SUM(COALESCE(quantity, 0)) AS order_quantity,
        SUM(COALESCE(discount_amount, 0)) AS subtotal_discount,

        0 AS refund_gross_revenue,
        0 AS refund_quantity,
        0 AS subtotal_refund

    FROM {{ ref('shopify_order_line_items') }}

    GROUP BY
        order_id,
        order_line_id,
        product_id,
        variant_id,
        title,
        variant_title,
        name,
        order_date,
        day,
        week,
        month,
        quarter,
        year,
        sku,
        fulfillment_status,
        gift_card

),

refund_data AS (

    SELECT
        order_id,
        order_line_id,
        product_id,
        variant_id,

        title,
        variant_title,
        name,

        refund_date AS transaction_date,

        day,
        week,
        month,
        quarter,
        year,

        sku,
        fulfillment_status,
        gift_card,

        0 AS order_gross_revenue,
        0 AS order_quantity,
        0 AS subtotal_discount,

        SUM(COALESCE(price * refund_quantity, 0)) AS refund_gross_revenue,
        SUM(COALESCE(refund_quantity, 0)) AS refund_quantity,
        SUM(COALESCE(refund_subtotal, 0)) AS subtotal_refund

    FROM {{ ref('shopify_refund_line_items') }}

    GROUP BY
        order_id,
        order_line_id,
        product_id,
        variant_id,
        title,
        variant_title,
        name,
        refund_date,
        day,
        week,
        month,
        quarter,
        year,
        sku,
        fulfillment_status,
        gift_card

),

sales_and_refunds_data AS (

    SELECT * FROM sales_data

    UNION ALL

    SELECT * FROM refund_data

),

shopify_data AS (

{% for granularity in date_granularity_list %}

SELECT

    '{{ granularity }}' AS date_granularity,

    {{ granularity }} AS date,

    product_id,
    variant_id,

    title,
    variant_title,
    name,

    sku,
    fulfillment_status,
    gift_card,

    -- SALES
    SUM(order_gross_revenue) AS order_gross_revenue,
    SUM(refund_gross_revenue) AS refund_gross_revenue,

    -- QUANTITIES
    SUM(order_quantity) AS order_quantity,
    SUM(refund_quantity) AS refund_quantity,

    -- DISCOUNTS / REFUNDS
    SUM(subtotal_discount) AS subtotal_discount,
    SUM(subtotal_refund) AS subtotal_refund,

    -- NET METRICS
    SUM(COALESCE(order_quantity, 0))
        - SUM(COALESCE(refund_quantity, 0)) AS net_quantity,

    SUM(COALESCE(order_gross_revenue, 0))
        - SUM(COALESCE(subtotal_discount, 0))
        - SUM(COALESCE(subtotal_refund, 0)) AS net_sales

FROM sales_and_refunds_data

GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10

{% if not loop.last %} UNION ALL {% endif %}

{% endfor %}

)

SELECT *
FROM shopify_data

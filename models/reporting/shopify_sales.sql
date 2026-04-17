{{ config(
    alias = target.database + '_shopify_sales'
) }}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH sales_and_refunds_data AS (

    -- SALES ROWS
    SELECT
        date,
        day,
        week,
        month,
        quarter,
        year,

        -- Orders
        1 AS orders,
        CASE WHEN customer_order_index = 1 THEN 1 ELSE 0 END AS first_orders,
        CASE WHEN customer_order_index > 1 THEN 1 ELSE 0 END AS repeat_orders,

        -- Gross Revenue
        COALESCE(gross_revenue,0) AS gross_revenue,
        CASE WHEN customer_order_index = 1 THEN COALESCE(gross_revenue,0) ELSE 0 END AS first_order_gross_revenue,
        CASE WHEN customer_order_index > 1 THEN COALESCE(gross_revenue,0) ELSE 0 END AS repeat_order_gross_revenue,

        -- Subtotal Discounts
        COALESCE(subtotal_discount,0) AS subtotal_discount,
        CASE WHEN customer_order_index = 1 THEN COALESCE(subtotal_discount,0) ELSE 0 END AS first_order_subtotal_discount,
        CASE WHEN customer_order_index > 1 THEN COALESCE(subtotal_discount,0) ELSE 0 END AS repeat_order_subtotal_discount,

        -- Subtotal Refunds
        0 AS subtotal_refund,
        0 AS first_order_subtotal_refund,
        0 AS repeat_order_subtotal_refund,

        -- Shipping Revenue
        COALESCE(shipping_price,0) AS shipping_revenue,
        CASE WHEN customer_order_index = 1 THEN COALESCE(shipping_price,0) ELSE 0 END AS first_order_shipping_revenue,
        CASE WHEN customer_order_index > 1 THEN COALESCE(shipping_price,0) ELSE 0 END AS repeat_order_shipping_revenue,

        -- Shipping Discounts
        COALESCE(shipping_discount,0) AS shipping_discounts,
        CASE WHEN customer_order_index = 1 THEN COALESCE(shipping_discount,0) ELSE 0 END AS first_order_shipping_discounts,
        CASE WHEN customer_order_index > 1 THEN COALESCE(shipping_discount,0) ELSE 0 END AS repeat_order_shipping_discounts,

        -- Shipping Refunds
        0 AS shipping_refunds,
        0 AS first_order_shipping_refunds,
        0 AS repeat_order_shipping_refunds,

        -- Tax Sales
        COALESCE(total_tax,0) AS tax_sales,
        CASE WHEN customer_order_index = 1 THEN COALESCE(total_tax,0) ELSE 0 END AS first_order_tax_sales,
        CASE WHEN customer_order_index > 1 THEN COALESCE(total_tax,0) ELSE 0 END AS repeat_order_tax_sales,

        -- Tax Refunds
        0 AS tax_refunds,
        0 AS first_order_tax_refunds,
        0 AS repeat_order_tax_refunds

    FROM {{ ref('shopify_daily_sales_by_order') }}
    WHERE cancelled_at IS NULL
      AND customer_id IS NOT NULL

    UNION ALL

    -- REFUND ROWS
    SELECT
        date,
        day,
        week,
        month,
        quarter,
        year,

        -- Orders
        0 AS orders,
        0 AS first_orders,
        0 AS repeat_orders,

        -- Gross Revenue
        0 AS gross_revenue,
        0 AS first_order_gross_revenue,
        0 AS repeat_order_gross_revenue,

        -- Subtotal Discounts
        0 AS subtotal_discount,
        0 AS first_order_subtotal_discount,
        0 AS repeat_order_subtotal_discount,

        -- Subtotal Refunds
        COALESCE(subtotal_refund,0) AS subtotal_refund,
        CASE WHEN customer_order_index = 1 THEN COALESCE(subtotal_refund,0) ELSE 0 END AS first_order_subtotal_refund,
        CASE WHEN customer_order_index > 1 THEN COALESCE(subtotal_refund,0) ELSE 0 END AS repeat_order_subtotal_refund,

        -- Shipping Revenue
        0 AS shipping_revenue,
        0 AS first_order_shipping_revenue,
        0 AS repeat_order_shipping_revenue,

        -- Shipping Discounts
        0 AS shipping_discounts,
        0 AS first_order_shipping_discounts,
        0 AS repeat_order_shipping_discounts,

        -- Shipping Refunds
        COALESCE(shipping_refund,0) AS shipping_refunds,
        CASE WHEN customer_order_index = 1 THEN COALESCE(shipping_refund,0) ELSE 0 END AS first_order_shipping_refunds,
        CASE WHEN customer_order_index > 1 THEN COALESCE(shipping_refund,0) ELSE 0 END AS repeat_order_shipping_refunds,

        -- Tax Sales
        0 AS tax_sales,
        0 AS first_order_tax_sales,
        0 AS repeat_order_tax_sales,

        -- Tax Refunds
        COALESCE(tax_refund,0) AS tax_refunds,
        CASE WHEN customer_order_index = 1 THEN COALESCE(tax_refund,0) ELSE 0 END AS first_order_tax_refunds,
        CASE WHEN customer_order_index > 1 THEN COALESCE(tax_refund,0) ELSE 0 END AS repeat_order_tax_refunds

    FROM {{ ref('shopify_daily_refunds') }}
    WHERE cancelled_at IS NULL

),

shopify_data AS (

{% for granularity in date_granularity_list %}

SELECT
    '{{ granularity }}' AS date_granularity,
    {{ granularity }} AS date,

    -- Orders
    SUM(orders) AS orders,
    SUM(first_orders) AS first_orders,
    SUM(repeat_orders) AS repeat_orders,

    -- Gross Revenue
    SUM(gross_revenue) AS gross_sales,
    SUM(first_order_gross_revenue) AS first_order_gross_sales,
    SUM(repeat_order_gross_revenue) AS repeat_order_gross_sales,

    -- Subtotal Discounts
    SUM(subtotal_discount) AS subtotal_discounts,
    SUM(first_order_subtotal_discount) AS first_order_subtotal_discounts,
    SUM(repeat_order_subtotal_discount) AS repeat_order_subtotal_discounts,

    -- Subtotal Refunds
    SUM(subtotal_refund) AS subtotal_refunds,
    SUM(first_order_subtotal_refund) AS first_order_subtotal_refunds,
    SUM(repeat_order_subtotal_refund) AS repeat_order_subtotal_refunds,

    -- Shipping Revenue
    SUM(shipping_revenue) AS shipping_revenue,
    SUM(first_order_shipping_revenue) AS first_order_shipping_revenue,
    SUM(repeat_order_shipping_revenue) AS repeat_order_shipping_revenue,

    -- Shipping Discounts
    SUM(shipping_discounts) AS shipping_discounts,
    SUM(first_order_shipping_discounts) AS first_order_shipping_discounts,
    SUM(repeat_order_shipping_discounts) AS repeat_order_shipping_discounts,

    -- Shipping Refunds
    SUM(shipping_refunds) AS shipping_refunds,
    SUM(first_order_shipping_refunds) AS first_order_shipping_refunds,
    SUM(repeat_order_shipping_refunds) AS repeat_order_shipping_refunds,

    -- Tax Sales
    SUM(tax_sales) AS tax_sales,
    SUM(first_order_tax_sales) AS first_order_tax_sales,
    SUM(repeat_order_tax_sales) AS repeat_order_tax_sales,

    -- Tax Refunds
    SUM(tax_refunds) AS tax_refunds,
    SUM(first_order_tax_refunds) AS first_order_tax_refunds,
    SUM(repeat_order_tax_refunds) AS repeat_order_tax_refunds,

    -- Net Sales
    SUM(gross_revenue) - SUM(subtotal_discount) + SUM(subtotal_refund) AS net_sales,
    SUM(first_order_gross_revenue) - SUM(first_order_subtotal_discount) + SUM(first_order_subtotal_refund) AS first_order_net_sales,
    SUM(repeat_order_gross_revenue) - SUM(repeat_order_subtotal_discount) + SUM(repeat_order_subtotal_refund) AS repeat_order_net_sales,

    -- Total Net Sales
    (
        SUM(gross_revenue)
        - SUM(subtotal_discount)
        + SUM(subtotal_refund)
        + SUM(shipping_revenue)
        - SUM(shipping_discounts)
        + SUM(shipping_refunds)
        + SUM(tax_sales)
        + SUM(tax_refunds)
    ) AS total_net_sales,

    (
        SUM(first_order_gross_revenue)
        - SUM(first_order_subtotal_discount)
        + SUM(first_order_subtotal_refund)
        + SUM(first_order_shipping_revenue)
        - SUM(first_order_shipping_discounts)
        + SUM(first_order_shipping_refunds)
        + SUM(first_order_tax_sales)
        + SUM(first_order_tax_refunds)
    ) AS first_order_total_net_sales,

    (
        SUM(repeat_order_gross_revenue)
        - SUM(repeat_order_subtotal_discount)
        + SUM(repeat_order_subtotal_refund)
        + SUM(repeat_order_shipping_revenue)
        - SUM(repeat_order_shipping_discounts)
        + SUM(repeat_order_shipping_refunds)
        + SUM(repeat_order_tax_sales)
        + SUM(repeat_order_tax_refunds)
    ) AS repeat_order_total_net_sales

FROM sales_and_refunds_data
GROUP BY 1,2

{% if not loop.last %} UNION ALL {% endif %}

{% endfor %}

)

SELECT *
FROM shopify_data

{{ config(
    alias = target.database + '_shopify_sales'
) }}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}


WITH shopify_base AS (

    SELECT
        date,
        order_id,
        customer_id,
        customer_order_index,

        -- SALES
        COALESCE(gross_revenue, 0) AS gross_revenue,
        COALESCE(subtotal_revenue, 0) AS subtotal_revenue,

        -- DISCOUNTS
        COALESCE(subtotal_discount, 0) AS subtotal_discount,
        COALESCE(shipping_discount, 0) AS shipping_discount,

        -- SHIPPING
        COALESCE(shipping_price, 0) AS shipping_price,

        -- TAX
        COALESCE(total_tax, 0) AS total_tax,

        -- REFUNDS
        COALESCE(subtotal_refund, 0) AS subtotal_refund,
        COALESCE(shipping_refund, 0) AS shipping_refund,
        COALESCE(tax_refund, 0) AS tax_refund

    FROM {{ ref('shopify_daily_sales_by_order') }}
    LEFT JOIN {{ ref('shopify_daily_refunds') }}
        USING (order_id, date)

    WHERE cancelled_at IS NULL
      AND customer_id IS NOT NULL
),

shopify_aggregated AS (

    {% for granularity in date_granularity_list %}

    SELECT

        '{{ granularity }}' AS date_granularity,
        {{ granularity }} AS date,

        customer_order_index,

        /* =========================
           ORDERS
        ========================= */
        COUNT(DISTINCT order_id) AS orders,
        COUNT(DISTINCT CASE WHEN customer_order_index = 1 THEN order_id END) AS new_orders,
        COUNT(DISTINCT CASE WHEN customer_order_index > 1 THEN order_id END) AS returning_orders,

        /* =========================
           GROSS SALES
        ========================= */
        SUM(gross_revenue) AS gross_sales,
        SUM(CASE WHEN customer_order_index = 1 THEN gross_revenue END) AS gross_sales_new,
        SUM(CASE WHEN customer_order_index > 1 THEN gross_revenue END) AS gross_sales_returning,

        /* =========================
           DISCOUNTS
        ========================= */
        SUM(subtotal_discount) AS subtotal_discounts,
        SUM(CASE WHEN customer_order_index = 1 THEN subtotal_discount END) AS subtotal_discounts_new,
        SUM(CASE WHEN customer_order_index > 1 THEN subtotal_discount END) AS subtotal_discounts_returning,

        /* =========================
           REFUNDS
        ========================= */
        SUM(subtotal_refund) AS subtotal_refunds,
        SUM(CASE WHEN customer_order_index = 1 THEN subtotal_refund END) AS subtotal_refunds_new,
        SUM(CASE WHEN customer_order_index > 1 THEN subtotal_refund END) AS subtotal_refunds_returning,

        /* SHIPPING */
        SUM(shipping_price) AS shipping_revenue,
        SUM(shipping_discount) AS shipping_discounts,
        SUM(shipping_refund) AS shipping_refunds,

        SUM(CASE WHEN customer_order_index = 1 THEN shipping_price END) AS shipping_revenue_new,
        SUM(CASE WHEN customer_order_index > 1 THEN shipping_price END) AS shipping_revenue_returning,

        /* TAX */
        SUM(total_tax) AS tax_sales,
        SUM(tax_refund) AS tax_refunds,

        SUM(CASE WHEN customer_order_index = 1 THEN total_tax END) AS tax_sales_new,
        SUM(CASE WHEN customer_order_index > 1 THEN total_tax END) AS tax_sales_returning

    FROM shopify_base

    GROUP BY 1,2,3

    {% if not loop.last %}UNION ALL{% endif %}

    {% endfor %}
),

shopify_final AS (

    SELECT

        *,

        /* =========================
           NET SALES CORE
        ========================= */
        (gross_sales - subtotal_discounts + subtotal_refunds) AS net_sales,
        (gross_sales_new - subtotal_discounts_new + subtotal_refunds_new) AS net_sales_new,
        (gross_sales_returning - subtotal_discounts_returning + subtotal_refunds_returning) AS net_sales_returning,

        /* =========================
           TOTAL NET SALES (FULL DEFINITION)
        ========================= */
        (
            (gross_sales - subtotal_discounts + subtotal_refunds)
            + shipping_revenue - shipping_discounts + shipping_refunds
            + tax_sales + tax_refunds
        ) AS total_net_sales,

        (
            (gross_sales_new - subtotal_discounts_new + subtotal_refunds_new)
            + shipping_revenue_new
            + tax_sales_new
        ) AS total_net_sales_new,

        (
            (gross_sales_returning - subtotal_discounts_returning + subtotal_refunds_returning)
            + shipping_revenue_returning
            + tax_sales_returning
        ) AS total_net_sales_returning

    FROM shopify_aggregated
)

SELECT *
FROM shopify_final

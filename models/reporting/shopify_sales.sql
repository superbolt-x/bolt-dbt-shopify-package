
{{ config (
    alias = target.database + '_blended_performance'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}


WITH 
    
sales_and_refunds_data AS(
    SELECT 
    date, 
    day, 
    week, 
    month, 
    quarter, 
    year,
    
    -- Orders -- 
    COUNT(order_id) as orders,
    COUNT(CASE WHEN customer_order_index = 1 THEN order_id END) as first_orders,
    COUNT(CASE WHEN customer_order_index > 1 THEN order_id END) as repeat_orders,
    
    -- Gross Revenue -- 
    gross_revenue, 
    COUNT(CASE WHEN customer_order_index = 1 THEN gross_revenue END) as first_order_gross_revenue,
    COUNT(CASE WHEN customer_order_index > 1 THEN gross_revenue END) as repeat_order_gross_revenue,

    -- Subtotal Discounts --
    subtotal_discount, 
    COUNT(CASE WHEN customer_order_index = 1 THEN subtotal_discount END) as first_order_subtotal_discount,
    COUNT(CASE WHEN customer_order_index > 1 THEN subtotal_discount END) as repeat_order_subtotal_discount,

    -- Subtotal Refunds --
    0 as subtotal_refund, 
    0 as first_order_subtotal_refund,
    0 as repeat_order_subtotal_refund,

    -- Shipping Revenue --
    shipping_price as shipping_revenue, 
    COUNT(CASE WHEN customer_order_index = 1 THEN shipping_price END) as first_order_shipping_revenue,
    COUNT(CASE WHEN customer_order_index > 1 THEN shipping_price END) as repeat_order_shipping_revenue,

    -- Shipping Discounts --
    shipping_discount as shipping_discounts, 
    COUNT(CASE WHEN customer_order_index = 1 THEN shipping_discount END) as first_order_shipping_discounts,
    COUNT(CASE WHEN customer_order_index > 1 THEN shipping_discount END) as repeat_order_shipping_discounts,

    -- Shipping Refunds --
    0 as shipping_refunds, 
    0 as first_order_shipping_refunds,
    0 as repeat_order_shipping_refunds,

    -- Tax Sales --
    total_tax as tax_sales, 
    COUNT(CASE WHEN customer_order_index = 1 THEN total_tax END) as first_order_tax_sales,
    COUNT(CASE WHEN customer_order_index > 1 THEN total_tax END) as repeat_order_tax_sales,
    
    -- Tax Refunds --
    0 as tax_refunds, 
    0 as first_order_tax_refunds,
    0 as repeat_order_tax_refunds,
    
    FROM {{ ref('shopify_daily_sales_by_order') }}
    UNION ALL
    SELECT 
    date, 
    day, 
    week, 
    month, 
    quarter, 
    year,

    -- Orders -- 
    0 as orders,
    0 as first_orders,
    0 as repeat_orders,
    
    -- Gross Revenue -- 
    0 as gross_revenue, 
    0 as first_order_gross_revenue,
    0 as repeat_order_gross_revenue,

    -- Subtotal Discounts --
    0 as subtotal_discount, 
    0 as first_order_subtotal_discount,
    0 as repeat_order_subtotal_discount,
    
    -- Subtotal Refunds --
    subtotal_refund, 
    COUNT(CASE WHEN customer_order_index = 1 THEN subtotal_refund END) as first_order_subtotal_refund,
    COUNT(CASE WHEN customer_order_index > 1 THEN subtotal_refund END) as repeat_order_subtotal_refund,

    -- Shipping Revenue --
    0 as shipping_revenue, 
    0 as first_order_shipping_revenue,
    0 as repeat_order_shipping_revenue,

    -- Shipping Discounts --
    0 as shipping_discounts, 
    0 as first_order_shipping_discounts,
    0 as repeat_order_shipping_discounts,

    -- Shipping Refunds --
    shipping_refund as shipping_refunds, 
    COUNT(CASE WHEN customer_order_index = 1 THEN shipping_refund END) as first_order_shipping_refunds,
    COUNT(CASE WHEN customer_order_index > 1 THEN shipping_refund END) as repeat_order_shipping_refunds,

    -- Tax Sales --
    0 as tax_sales, 
    0 as first_order_tax_sales,
    0 as repeat_order_tax_sales,

    -- Tax Refunds --
    tax_refund as tax_refunds, 
    COUNT(CASE WHEN customer_order_index = 1 THEN tax_refund END) as first_order_tax_refunds,
    COUNT(CASE WHEN customer_order_index > 1 THEN tax_refund END) as repeat_order_tax_refunds,
    
    FROM {{ ref('shopify_daily_refunds') }}
    

shopify_data AS (
    {% for granularity in date_granularity_list %}
    SELECT 
    '{{granularity}}' as date_granularity,
    {{granularity}} as date,
    
    -- Orders -- 
    orders,
    first_orders,
    repeat_orders,
    
    -- Gross Revenue -- 
    gross_revenue, 
    first_order_gross_revenue,
    repeat_order_gross_revenue,

    -- Subtotal Discounts --
    subtotal_discount, 
    first_order_subtotal_discount,
    repeat_order_subtotal_discount,

    -- Subtotal Refunds --
    subtotal_refund, 
    first_order_subtotal_refund,
    repeat_order_subtotal_refund,

    -- Shipping Revenue --
    shipping_revenue, 
    first_order_shipping_revenue,
    repeat_order_shipping_revenue,

    -- Shipping Discounts --
    shipping_discounts, 
    first_order_shipping_discounts,
    repeat_order_shipping_discounts,

    -- Shipping Refunds --
    shipping_refunds, 
    first_order_shipping_refunds,
    repeat_order_shipping_refunds,

    -- Tax Sales --
    tax_sales, 
    first_order_tax_sales,
    repeat_order_tax_sales,
    
    -- Tax Refunds --
    tax_refunds, 
    first_order_tax_refunds,
    repeat_order_tax_refunds,

    -- Net Sales --
    gross_revenue - subtotal_discount + subtotal_refund as net_sales, 
    first_order_gross_revenue - first_order_subtotal_discount + first_order_subtotal_refund as first_order_net_sales, ,
    repeat_order_gross_revenue - repeat_order_subtotal_discount + repeat_order_subtotal_refund as repeat_order_net_sales, 

    -- Total Net Sales --
    net_sales + shipping_revenue - shipping_discounts + shipping_refunds + tax_sales + tax_refunds as total_net_sales 
    first_order_net_sales + first_order_shipping_revenue - first_order_shipping_discounts + first_order_shipping_refunds + first_order_tax_sales + first_order_tax_refunds as first_order_total_net_sales 
    repeat_order_net_sales + repeat_order_shipping_revenue - repeat_order_shipping_discounts + repeat_order_shipping_refunds + repeat_order_tax_sales + repeat_order_tax_refunds as repeat_order_total_net_sales 
    

    
    FROM sales_and_refunds_data
    GROUP BY date_granularity, {{granularity}}
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
    )

select * from shopify_data

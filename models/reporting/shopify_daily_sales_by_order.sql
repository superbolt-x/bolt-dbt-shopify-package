{{ config (
    alias = target.database + '_shopify_daily_sales_by_order',
    materialized='incremental',
    unique_key='unique_key'
)}}

{# -------------------- VARS -------------------- #}
{%- set sales_channel_exclusion = var("sales_channel_exclusion", "") -%}
{%- set sales_channel_inclusion = var("sales_channel_inclusion", "") -%}
{%- set shipping_countries_excluded = var("shipping_countries_excluded", "") -%}
{%- set shipping_countries_included = var("shipping_countries_included", "") -%}
{%- set order_tags_keyword_exclusion = var("order_tags_keyword_exclusion", "") -%}
{%- set order_tags_keyword_inclusion = var("order_tags_keyword_inclusion", "") -%}
{%- set email_address_exclusion = var("email_address_exclusion", "") -%}

{# -------------------- LISTS -------------------- #}
{%- set sales_channel_exclusion_list =
    "'" ~ sales_channel_exclusion.split('|') | reject('equalto','') | join("','") ~ "'"
    if sales_channel_exclusion | trim else none
-%}

{%- set sales_channel_inclusion_list =
    "'" ~ sales_channel_inclusion.split('|') | reject('equalto','') | join("','") ~ "'"
    if sales_channel_inclusion | trim else none
-%}

{%- set shipping_country_exclusion_list =
    "'" ~ shipping_countries_excluded.split('|') | reject('equalto','') | join("','") ~ "'"
    if shipping_countries_excluded | trim else none
-%}

{%- set shipping_country_inclusion_list =
    "'" ~ shipping_countries_included.split('|') | reject('equalto','') | join("','") ~ "'"
    if shipping_countries_included | trim else none
-%}

WITH giftcard_deduction AS (
    SELECT 
        order_id, 
        CASE WHEN items_count = giftcard_count THEN 'true' ELSE 'false' END AS giftcard_only,
        giftcard_deduction
    FROM (
        SELECT 
            order_id,
            SUM(quantity) AS items_count,
            COALESCE(SUM(CASE WHEN gift_card IS TRUE THEN quantity END),0) AS giftcard_count,
            COALESCE(SUM(CASE WHEN gift_card IS TRUE THEN price * quantity END),0) AS giftcard_deduction
        FROM {{ ref('shopify_line_items') }}
        GROUP BY 1
    )
),

orders AS (
    SELECT 
        order_date AS date,
        cancelled_at::date AS cancelled_at,
        customer_first_order_date AS customer_acquisition_date,
        order_id,
        customer_id,
        customer_order_index,
        gross_revenue - COALESCE(giftcard_deduction,0) AS gross_revenue,
        total_discounts - gross_revenue + subtotal_revenue AS shipping_discount,
        gross_revenue - subtotal_revenue AS subtotal_discount,
        discount_rate,
        subtotal_revenue,
        total_tax,
        shipping_price,
        total_revenue,
        order_tags,
        email,
        source_name,
        shipping_address_country_code
    FROM {{ ref('shopify_orders') }}
    LEFT JOIN giftcard_deduction USING(order_id)
    WHERE giftcard_only = 'false'

    {# -------- SALES CHANNEL -------- #}
    {% if sales_channel_inclusion_list %}
        AND source_name IN ({{ sales_channel_inclusion_list }})
    {% elif sales_channel_exclusion_list %}
        AND (source_name NOT IN ({{ sales_channel_exclusion_list }}) OR source_name IS NULL)
    {% endif %}

    {# -------- TAGS -------- #}
    {% if order_tags_keyword_exclusion | trim %}
        AND (order_tags !~* '{{ order_tags_keyword_exclusion }}' OR order_tags IS NULL)
    {% endif %}

    {% if order_tags_keyword_inclusion | trim %}
        AND order_tags ~* '{{ order_tags_keyword_inclusion }}'
    {% endif %}

    {# -------- EMAIL -------- #}
    {% if email_address_exclusion | trim %}
        AND (email !~* '{{ email_address_exclusion }}' OR email IS NULL)
    {% endif %}

    {# -------- SHIPPING COUNTRY -------- #}
    {% if shipping_country_inclusion_list %}
        AND shipping_address_country_code IN ({{ shipping_country_inclusion_list }})
    {% elif shipping_country_exclusion_list %}
        AND (shipping_address_country_code NOT IN ({{ shipping_country_exclusion_list }}) OR shipping_address_country_code IS NULL)
    {% endif %}
)

SELECT *,
    {{ get_date_parts('date') }},
    date || '_' || order_id AS unique_key
FROM orders

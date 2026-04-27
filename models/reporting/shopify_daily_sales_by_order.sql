{{ config(
    alias = target.database + '_shopify_daily_sales_by_order',
    materialized='incremental',
    unique_key='unique_key'
) }}

{# -------------------- VARS -------------------- #}
{%- set sales_channel_exclusion = var("sales_channel_exclusion", "") | trim -%}
{%- set sales_channel_inclusion = var("sales_channel_inclusion", "") | trim -%}

{%- set shipping_countries_excluded = var("shipping_countries_excluded", "") | trim -%}
{%- set shipping_countries_included = var("shipping_countries_included", "") | trim -%}

{%- set order_tags_keyword_exclusion = var("order_tags_keyword_exclusion", "") | trim -%}
{%- set order_tags_keyword_inclusion = var("order_tags_keyword_inclusion", "") | trim -%}

{%- set email_address_exclusion = var("email_address_exclusion", "") | trim -%}

WITH giftcard_deduction AS (
    SELECT 
        order_id,
        CASE 
            WHEN items_count = giftcard_count THEN 'true' 
            ELSE 'false' 
        END AS giftcard_only,
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

    {# -------------------- SALES CHANNEL -------------------- #}
    {% if sales_channel_inclusion %}
        AND source_name IN (
            '{{ sales_channel_inclusion | replace("|", "','") }}'
        )
    {% elif sales_channel_exclusion %}
        AND source_name NOT IN (
            '{{ sales_channel_exclusion | replace("|", "','") }}'
        )
    {% endif %}

    {# -------------------- SHIPPING COUNTRY -------------------- #}
    {% if shipping_countries_included %}
        AND shipping_address_country_code IN (
            '{{ shipping_countries_included | replace("|", "','") }}'
        )
    {% elif shipping_countries_excluded %}
        AND shipping_address_country_code NOT IN (
            '{{ shipping_countries_excluded | replace("|", "','") }}'
        )
    {% endif %}

    {# -------------------- TAGS -------------------- #}
    {% if order_tags_keyword_exclusion %}
        AND (order_tags !~* '{{ order_tags_keyword_exclusion }}' OR order_tags IS NULL)
    {% endif %}

    {% if order_tags_keyword_inclusion %}
        AND order_tags ~* '{{ order_tags_keyword_inclusion }}'
    {% endif %}

    {# -------------------- EMAIL -------------------- #}
    {% if email_address_exclusion %}
        AND (email !~* '{{ email_address_exclusion }}' OR email IS NULL)
    {% endif %}
)

SELECT *,
    {{ get_date_parts('date') }},
    date || '_' || order_id AS unique_key
FROM orders

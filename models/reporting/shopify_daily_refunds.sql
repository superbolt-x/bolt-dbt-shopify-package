{{ config (
    alias = target.database + '_shopify_daily_refunds'
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
{%- set sales_channel_exclusion_values =
    sales_channel_exclusion.split('|') | reject('equalto','') | list
-%}

{%- set sales_channel_exclusion_list =
    "'" ~ sales_channel_exclusion_values | join("','") ~ "'"
    if sales_channel_exclusion_values | length > 0 else none
-%}

{%- set sales_channel_inclusion_values =
    sales_channel_inclusion.split('|') | reject('equalto','') | list
-%}

{%- set sales_channel_inclusion_list =
    "'" ~ sales_channel_inclusion_values | join("','") ~ "'"
    if sales_channel_inclusion_values | length > 0 else none
-%}

{%- set shipping_country_exclusion_values =
    shipping_countries_excluded.split('|') | reject('equalto','') | list
-%}

{%- set shipping_country_exclusion_list =
    "'" ~ shipping_country_exclusion_values | join("','") ~ "'"
    if shipping_country_exclusion_values | length > 0 else none
-%}

{%- set shipping_country_inclusion_values =
    shipping_countries_included.split('|') | reject('equalto','') | list
-%}

{%- set shipping_country_inclusion_list =
    "'" ~ shipping_country_inclusion_values | join("','") ~ "'"
    if shipping_country_inclusion_values | length > 0 else none
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

refunds AS (
    SELECT 
        refund_date::date AS date,
        refund_id,
        order_id,

        SUM(
            CASE 
                WHEN giftcard_only = 'true' THEN 0
                ELSE subtotal_refund - amount_discrepancy_refund
            END
        ) AS subtotal_refund,

        SUM(amount_shipping_refund) AS shipping_refund,

        SUM(total_tax_refund)
        + SUM(tax_amount_discrepancy_refund)
        + SUM(tax_amount_shipping_refund) AS tax_refund
    FROM {{ ref('shopify_refunds') }}
    LEFT JOIN giftcard_deduction USING(order_id)

    GROUP BY refund_date::date, refund_id, order_id
),

order_customer AS (
    SELECT 
        order_id,
        customer_id,
        cancelled_at,
        customer_order_index,
        order_tags,
        email,
        source_name,
        shipping_address_country_code
    FROM {{ ref('shopify_orders') }}

    {# -------- SALES CHANNEL -------- #}
    {% if sales_channel_inclusion_list %}
        WHERE source_name IN ({{ sales_channel_inclusion_list }})
    {% elif sales_channel_exclusion_list %}
        WHERE (source_name NOT IN ({{ sales_channel_exclusion_list }}) OR source_name IS NULL)
    {% else %}
        WHERE 1=1
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

    {# -------- SHIPPING COUNTRY FILTER -------- #}
    {% if shipping_country_inclusion_list %}
        AND shipping_address_country_code IN ({{ shipping_country_inclusion_list }})
    {% elif shipping_country_exclusion_list %}
        AND (shipping_address_country_code NOT IN ({{ shipping_country_exclusion_list }}) OR shipping_address_country_code IS NULL)
    {% endif %}
)

SELECT *,
    {{ get_date_parts('date') }}
FROM order_customer
LEFT JOIN refunds USING(order_id)

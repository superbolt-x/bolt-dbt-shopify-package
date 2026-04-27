{{ config (
    alias = target.database + '_shopify_daily_refunds'
)}}

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

    {# -------------------- SALES CHANNEL -------------------- #}
    {% if sales_channel_inclusion %}
        WHERE source_name IN (
            '{{ sales_channel_inclusion | replace("|", "','") }}'
        )
    {% elif sales_channel_exclusion %}
        WHERE source_name NOT IN (
            '{{ sales_channel_exclusion | replace("|", "','") }}'
        )
    {% else %}
        WHERE 1=1
    {% endif %}

    {# -------------------- TAG FILTERS -------------------- #}
    {% if order_tags_keyword_exclusion %}
        AND (order_tags !~* '{{ order_tags_keyword_exclusion }}' OR order_tags IS NULL)
    {% endif %}

    {% if order_tags_keyword_inclusion %}
        AND order_tags ~* '{{ order_tags_keyword_inclusion }}'
    {% endif %}

    {# -------------------- EMAIL FILTER -------------------- #}
    {% if email_address_exclusion %}
        AND (email !~* '{{ email_address_exclusion }}' OR email IS NULL)
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
)

SELECT *,
    {{ get_date_parts('date') }}
FROM order_customer
LEFT JOIN refunds USING(order_id)

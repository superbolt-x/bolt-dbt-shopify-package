{%- set schema_name,
        item_table_name,
        item_refund_table_name
        = 'shopify_raw', 'order_line', 'order_line_refund' -%}

{%- set item_selected_fields = [
    "order_id",
    "id",
    "product_id",
    "variant_id",
    "title",
    "variant_title",
    "name",
    "price",
    "quantity",
    "sku",
    "fulfillable_quantity",
    "fulfillment_status",
    "gift_card",
    "index"
] -%}

{%- set item_refund_selected_fields = [
    "id",
    "order_line_id",
    "refund_id",
    "quantity",
    "subtotal"
] -%}

{%- set order_line_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'order_line') -%}
{%- set order_line_refund_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'order_line_refund') -%}

WITH order_line_raw_data AS (

    {{ dbt_utils.union_relations(relations = order_line_raw_tables) }}

),

orders AS (

    SELECT 
        r.id AS order_line_id,

        {% for column in item_selected_fields -%}
        {{ get_shopify_clean_field(item_table_name, column) }}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM order_line_raw_data r
    LEFT JOIN {{ ref('shopify_orders') }} s
        ON r.order_id = s.order_id

),

order_line_refund_raw_data AS (

    {{ dbt_utils.union_relations(relations = order_line_refund_raw_tables) }}

),

refund_raw AS (

    SELECT 
        
        {% for column in item_refund_selected_fields -%}
        {{ get_shopify_clean_field(item_refund_table_name, column) }}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM order_line_refund_raw_data

),

refund AS (

    SELECT 
        r.order_line_id,

        s.refund_date,
        s.day,
        s.week,
        s.month,
        s.quarter,
        s.year,

        SUM(r.quantity) AS refund_quantity,
        SUM(r.subtotal) AS refund_subtotal

    FROM refund_raw r

    LEFT JOIN {{ ref('shopify_refunds') }} s
        ON r.refund_id = s.refund_id

    GROUP BY
        r.order_line_id,
        s.refund_date,
        s.day,
        s.week,
        s.month,
        s.quarter,
        s.year

)

SELECT 
    r.refund_date,
    r.day,
    r.week,
    r.month,
    r.quarter,
    r.year,

    r.refund_quantity,
    r.refund_subtotal,

    o.order_line_id,
    o.order_id,
    o.id,
    o.product_id,
    o.variant_id,
    o.title,
    o.variant_title,
    o.name,
    o.price,
    o.sku,
    o.fulfillable_quantity,
    o.fulfillment_status,
    o.gift_card,
    o.index

FROM orders o
LEFT JOIN refund r
    USING (order_line_id)


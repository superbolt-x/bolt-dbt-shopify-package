{%- set schema_name,
        item_table_name, 
        item_discount_table_name
        = 'shopify_raw', 'order_line', 'discount_allocation' -%}

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

{%- set item_discount_selected_fields = [
    "order_line_id",
    "amount"
] -%}

{%- set order_line_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'order_line') -%}
{%- set discount_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'discount_allocation') -%}

WITH order_line_raw_data AS (

    {{ dbt_utils.union_relations(relations = order_line_raw_tables) }}

),

orders AS (

    SELECT 
        s.order_date,
        s.day,
        s.week,
        s.month,
        s.quarter,
        s.year,

        r.id AS order_line_id,

        {% for column in item_selected_fields -%}
        {{ get_shopify_clean_field(item_table_name, column) }}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM order_line_raw_data r
    LEFT JOIN {{ ref('shopify_orders') }} s
        ON r.order_id = s.order_id

),

discount_raw_data AS (

    {{ dbt_utils.union_relations(relations = discount_raw_tables) }}

),

discount AS (

    SELECT 
        order_line_id,
        SUM(amount) AS discount_amount

    FROM (

        SELECT
        
            {% for column in item_discount_selected_fields -%}
            {{ get_shopify_clean_field(item_discount_table_name, column) }}
            {%- if not loop.last %},{% endif %}
            {% endfor %}

        FROM discount_raw_data

    )

    GROUP BY 1

)

SELECT 
    o.order_line_id,
    o.order_id,
    o.id,
    o.order_date,
    o.day,
    o.week,
    o.month,
    o.quarter,
    o.year,
    o.product_id,
    o.variant_id,
    o.title,
    o.variant_title,
    o.name,
    o.price,
    o.quantity,
    o.sku,
    o.fulfillable_quantity,
    o.fulfillment_status,
    o.gift_card,
    o.index,

    COALESCE(d.discount_amount, 0) AS discount_amount

FROM orders o
LEFT JOIN discount d
    USING (order_line_id)

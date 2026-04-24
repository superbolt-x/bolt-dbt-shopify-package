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
"amount",
] -%}

{%- set order_line_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'order_line') -%}
{%- set order_line_discount_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'discount_allocation') -%}

WITH order_line_raw_data AS 
    ({{ dbt_utils.union_relations(relations = order_line_raw_tables) }}),
    items AS 
    (SELECT 
        {% for column in item_selected_fields -%}
        {{ get_shopify_clean_field(item_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM order_line_raw_data
    )

order_line_discount_raw_data AS 
        ({{ dbt_utils.union_relations(relations = order_line_discount_raw_tables) }}),

    discount AS 
    (SELECT 
        
        {% for column in item_discount_selected_fields -%}
        {{ get_shopify_clean_field(item_discount_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM order_line_discount_raw_data
    ),

SELECT *
FROM items 
left join discount using (order_line_id)


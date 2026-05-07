{%- set schema_name,
        item_table_name, 
        item_discount_table_name,
        item_fund_table_name
        = 'shopify_raw', 'order_line', 'discount_allocation', 'order_line_refund' -%}
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

{%- set item_refund_selected_fields = [
"order_line_id",
"refund_id",
"quantity",
"subtotal"
] -%}

{%- set order_line_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'order_line') -%}
{%- set discount_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'discount_allocation') -%}
{%- set order_line_refund_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'order_line_refund') -%}


WITH order_line_raw_data AS 
    ({{ dbt_utils.union_relations(relations = order_line_raw_tables) }}),
        
    orders_raw AS 
    (SELECT 
        order_date,
        {% for column in item_selected_fields -%}
        {{ get_shopify_clean_field(item_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM order_line_raw_data r
        left join {{ ref('shopify_orders') }} s
        on r.order_id = s.order_id
    ),

discount_raw_data AS 
        ({{ dbt_utils.union_relations(relations = discount_raw_tables) }}),

    discount AS 
    (SELECT 
        
        {% for column in item_discount_selected_fields -%}
        {{ get_shopify_clean_field(item_discount_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM discount_raw_data
    ),



SELECT 
        order_line_id,
        id,
        order_date as date,
        product_id,
        variant_id,
        title,
        variant_title,
        name,
        price,
        quantity,
        sku,
        fulfillable_quantity,
        fulfillment_status,
        gift_card,
        index,
        amount as discount_amount,
        
FROM items 
left join discount using (order_line_id)
left join refund using (order_line_id)


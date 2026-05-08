{%- set schema_name, item_refund_table_name, item_refund_table_name = 'shopify_raw', 'order_line', 'order_line_refund' -%}
  
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

  
  WITH order_line_raw_data AS 
    ({{ dbt_utils.union_relations(relations = order_line_raw_tables) }}),
        
    orders AS 
    (SELECT 
        {% for column in item_selected_fields -%}
        {{ get_shopify_clean_field(item_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM order_line_raw_data r
        left join {{ ref('shopify_orders') }} s
        on r.order_id = s.order_id
    ),
  
order_line_refund_raw_data AS 
    ({{ dbt_utils.union_relations(relations = order_line_refund_raw_tables) }}),
        
    refund_raw AS 
    (SELECT 
        
        {% for column in item_refund_selected_fields -%}
        {{ get_shopify_clean_field(item_refund_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM order_line_refund_raw_data
    ),
        refund AS 
    (SELECT 
        order_line_id,
        refund_date,
        day,
        week,
        month,
        quarter,
        year,
        SUM(quantity) as refund_quantity,
        SUM(subtotal) as refund_subtotal
    FROM refund_raw r
        left join {{ ref('shopify_refunds') }} s
        on r.refund_id = s.refund_id
    GROUP BY order_line_id, refund_date
    )
SELECT 
        order_line_id,
        id,
        refund_date,
        day,
        week,
        month,
        quarter,
        year,
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
FROM orders 
  left join refund using (order_line_id)


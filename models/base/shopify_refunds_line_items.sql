{%- set schema_name,item_refund_table_name = 'shopify_raw', 'order_line_refund' -%}
  


{%- set item_refund_selected_fields = [
  "id",
"order_line_id",
"refund_id",
"quantity",
"subtotal"
] -%}


{%- set order_line_refund_raw_tables = dbt_utils.get_relations_by_pattern('shopify_raw%', 'order_line_refund') -%}

with
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
        refund_date,
        day,
        week,
        month,
        quarter,
        year,
        refund_quantity,
        refund_subtotal
FROM refund 

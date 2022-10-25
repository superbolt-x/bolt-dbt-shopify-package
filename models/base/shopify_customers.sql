{%- set selected_fields = [
    "id",
    "first_name",
    "last_name",    
    "email",
    "created_at"
] -%}

{%- set schema_name,
        customer_table_name,
        customer_tag_table_name
        = 'shopify_raw', 'customer','customer_tag' -%}

WITH customers AS 
    (SELECT 

        {% for column in selected_fields -%}
        {{ get_shopify_clean_field(customer_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, customer_table_name) }}
    ),

    tags AS 
    (SELECT customer_id, LISTAGG(value, ', ') WITHIN GROUP (ORDER BY index) as customer_tags
    FROM {{ source(schema_name, customer_tag_table_name) }}
    GROUP BY customer_id
    )


SELECT *,
    customer_id as unique_key
FROM customers 
LEFT JOIN tags USING(customer_id)
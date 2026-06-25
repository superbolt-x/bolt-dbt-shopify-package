{{ config(
    alias = target.database + '_shopify_cohort_orders',
    materialized = 'table'
) }}

{%- set granularities = [
    {'name': 'day',     'divisor': 1},
    {'name': 'week',    'divisor': 7},
    {'name': 'month',   'divisor': 30.42},
    {'name': 'quarter', 'divisor': 91.25}
] -%}
{%- set max_retention = 15 -%}

WITH line_items_adjusted AS (
    SELECT
        customer_id,
        customer_order_index,
        product_title,
        product_type,
        price,
        CASE
            WHEN product_title = 'Carbon Neutral Order' THEN NULL
            WHEN MAX(
                    CASE WHEN (product_title = 'Carbon Neutral Order' OR lower(product_title) LIKE '%sample%')
                              AND index = 1 THEN 1 ELSE 0 END
                 ) OVER (PARTITION BY order_id) = 1 THEN index - 1
            ELSE index
        END AS adjusted_line_item_index
    FROM {{ ref('shopify_daily_sales_by_order_line_item') }}
),

first_product AS (
    SELECT DISTINCT
        customer_id,
        product_type AS first_order_product_type
    FROM line_items_adjusted
    WHERE customer_order_index = 1
      AND adjusted_line_item_index = 1
      AND price > 0
),

orders_base AS (
    SELECT
        o.order_id,
        o.date                            AS order_date,
        o.customer_id,
        o.customer_acquisition_date::date AS customer_acquisition_day,
        o.customer_order_index,
        o.subtotal_revenue,
        fp.first_order_product_type
    FROM {{ ref('shopify_daily_sales_by_order') }} o
    LEFT JOIN first_product fp USING(customer_id)
    WHERE o.cancelled_at IS NULL
      AND o.customer_id IS NOT NULL
      AND o.customer_acquisition_date IS NOT NULL
)

{% for g in granularities -%}

, cohort_{{g.name}}_raw AS (
    SELECT
        '{{g.name}}'                                                                              AS date_granularity,
        DATE_TRUNC('{{g.name}}', customer_acquisition_day)                                        AS cohort,
        FLOOR(DATEDIFF(day, customer_acquisition_day, order_date) / {{g.divisor}})                AS retention,
        first_order_product_type,
        COUNT(CASE WHEN customer_order_index = 1 THEN customer_id END)                            AS new_customers_period,
        SUM(CASE WHEN customer_order_index = 1 THEN COALESCE(subtotal_revenue, 0) ELSE 0 END)     AS first_order_revenue_period,
        COUNT(CASE WHEN customer_order_index = 2 THEN customer_id END)                            AS second_orders_period,
        SUM(COALESCE(subtotal_revenue, 0))                                                        AS revenue_period
    FROM orders_base
    WHERE FLOOR(DATEDIFF(day, customer_acquisition_day, order_date) / {{g.divisor}}) >= 0
      AND FLOOR(DATEDIFF(day, customer_acquisition_day, order_date) / {{g.divisor}}) < {{max_retention}}
    GROUP BY 1, 2, 3, 4
),

cohort_{{g.name}} AS (
    SELECT
        date_granularity,
        cohort,
        retention,
        first_order_product_type,
        -- new_customers and aov are cohort-level constants, repeated across retention rows
        SUM(new_customers_period)
            OVER (PARTITION BY cohort, first_order_product_type)                                  AS new_customers,
        COALESCE(
            SUM(first_order_revenue_period)
                OVER (PARTITION BY cohort, first_order_product_type)
            / NULLIF(SUM(new_customers_period)
                OVER (PARTITION BY cohort, first_order_product_type), 0),
        0)                                                                                        AS aov,
        -- arpu and repeat_rate accumulate as retention increases
        COALESCE(
            SUM(revenue_period)
                OVER (PARTITION BY cohort, first_order_product_type
                      ORDER BY retention
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::decimal
            / NULLIF(SUM(new_customers_period)
                OVER (PARTITION BY cohort, first_order_product_type), 0),
        0)                                                                                        AS arpu,
        COALESCE(
            SUM(second_orders_period)
                OVER (PARTITION BY cohort, first_order_product_type
                      ORDER BY retention
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::float
            / NULLIF(SUM(new_customers_period)
                OVER (PARTITION BY cohort, first_order_product_type), 0),
        0)                                                                                        AS repeat_rate
    FROM cohort_{{g.name}}_raw
)

{%- endfor %}

{% for g in granularities -%}
SELECT date_granularity, cohort, retention, first_order_product_type, new_customers, aov, arpu, repeat_rate
FROM cohort_{{g.name}}
{%- if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}

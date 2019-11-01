-- Logic for the OVER clause inside window functions used by /customers/... dbt models,
-- whenever the ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING clause is required
{%- macro customer_window_over(partition_by, order_by, order_by_sort) -%}
OVER (PARTITION BY {{ partition_by }} ORDER BY {{ order_by }} {{ order_by_sort }} ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
{%- endmacro -%}

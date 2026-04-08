{{ config(materialized='table') }}
SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM {{ source('raw', 'olist_customers_dataset') }}
UNION  
SELECT t1.customer_id, t1.customer_id AS customer_unique_id,
	'Unknown' AS customer_city,
	'Unknown' AS customer_state
FROM {{source('raw','olist_orders_dataset')}} t1
	LEFT JOIN {{source('raw','olist_customers_dataset')}} t2 ON t1.customer_id = t2.customer_id
WHERE t2.customer_id IS NULL
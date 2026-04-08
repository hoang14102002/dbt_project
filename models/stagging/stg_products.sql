{{ config(materialized='table') }}
SELECT 
	product_id, product_category_name
FROM {{source('raw','olist_products_dataset')}}
WHERE product_category_name IS NOT NULL
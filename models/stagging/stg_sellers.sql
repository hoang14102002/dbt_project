{{ config(materialized='table') }}
SELECT 
	seller_id, seller_city, seller_state
FROM {{source('raw','olist_sellers_dataset')}}
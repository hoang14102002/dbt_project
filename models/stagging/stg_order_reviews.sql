{{ config(materialized='table') }}
SELECT DISTINCT
	review_id, 
	order_id, review_score,
	review_creation_date,
	review_answer_timestamp
FROM {{source('raw','olist_order_reviews_dataset')}}
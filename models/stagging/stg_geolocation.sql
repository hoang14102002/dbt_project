{{ config(materialized='table') }}
SELECT DISTINCT
	geolocation_zip_code_prefix,
	geolocation_state, 
	geolocation_city
FROM {{source('raw','olist_geolocation_dataset')}}

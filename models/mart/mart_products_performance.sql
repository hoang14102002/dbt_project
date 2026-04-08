WITH lastest_order_day AS 
(
	SELECT product_id, order_purchase_timestamp,
		ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY order_purchase_timestamp DESC) AS rn
	FROM {{ref('int_order_details')}} 
)
SELECT 
	t1.product_id, ISNULL(MAX(t2.product_category_name),'Unknown') AS product_category_name,
	COUNT(DISTINCT t1.customer_id) AS total_customer,
	COUNT(DISTINCT t1.order_id) AS total_order,
	SUM(t1.quantity) AS total_quantity, 
	SUM(t1.total_amount) AS total_amount,
	ROUND(SUM(t1.total_amount)/SUM(t1.quantity),2) AS avg_price,
	MAX(t1.price) AS max_price,
	MIN(t1.price) AS min_price,
	SUM(t1.freight_value) AS total_freight_value,
	ROUND(SUM(t1.freight_value)/SUM(t1.quantity),2) AS avt_freight_value,
	AVG(DATEDIFF(DAY,t1.order_purchase_timestamp, t1.order_delivered_customer_date)) AS avg_shipping_date,
	ROUND(SUM(CASE WHEN t1.order_status = 'canceled' THEN 1 ELSE 0 END) / COUNT(DISTINCT t1.order_id),2) AS cancel_rate,
	MAX(t3.order_purchase_timestamp) AS lastest_order_day
FROM {{ref('int_order_details')}} t1
	LEFT JOIN {{ref('stg_products')}} t2 ON t1.product_id = t2.product_id
	LEFT JOIN lastest_order_day t3 ON t1.product_id = t3.product_id AND t3.rn = 1
GROUP BY t1.product_id


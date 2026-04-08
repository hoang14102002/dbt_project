WITH lastest_order_day AS 
(
	SELECT seller_id, order_purchase_timestamp,
		ROW_NUMBER() OVER(PARTITION BY seller_id ORDER BY order_purchase_timestamp DESC) AS rn
	FROM {{ref('int_order_details')}} 
), 
order_value AS 
(
    SELECT seller_id, order_id, SUM(total_amount) AS total_amount
	FROM {{ref('int_order_details')}} 
    GROUP BY seller_id, order_id 
),
group_order_value AS
(
    SELECT seller_id, MAX(total_amount) AS max_total_amount ,
	    MIN(total_amount) AS min_total_amount 
	FROM order_value
	GROUP BY seller_id
)
SELECT 
	t1.seller_id,
	COUNT(DISTINCT t1.customer_id) AS total_customer,
	COUNT(DISTINCT t1.order_id) AS total_order,
    COUNT(DISTINCT t1.product_id) AS total_product,
	SUM(t1.quantity) AS total_product_quantity, 
	SUM(t1.total_amount) AS total_order_amount,
    MAX(t2.max_total_amount) AS max_order_amount,
    MIN(t2.min_total_amount) AS min_order_amount,
	ROUND(SUM(t1.total_amount)/SUM(t1.quantity),2) AS avg_product_price,
	MAX(t1.price) AS max_product_price,
	MIN(t1.price) AS min_product_price,
	SUM(t1.freight_value) AS total_freight_value,
	ROUND(SUM(t1.freight_value)/SUM(t1.quantity),2) AS avt_freight_value,
	ROUND(SUM(CASE WHEN t1.order_status = 'canceled' THEN 1 ELSE 0 END) / COUNT(DISTINCT t1.order_id),2) AS cancel_rate,
	MAX(t3.order_purchase_timestamp) AS lastest_order_day
FROM {{ref('int_order_details')}} t1
    LEFT JOIN group_order_value AS t2 ON t1.seller_id = t2.seller_id
	LEFT JOIN lastest_order_day t3 ON t1.seller_id = t3.seller_id AND t3.rn = 1
GROUP BY t1.seller_id


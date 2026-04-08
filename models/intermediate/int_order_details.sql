SELECT	
	t1.order_id, MAX(t1.seller_id) AS seller_id,
	t1.product_id, COUNT(DISTINCT t1.product_id) AS quantity,  MAX(t1.price) AS price, 
	MAX(t1.freight_value) AS freight_value,
	MAX(t1.price) * COUNT(DISTINCT t1.product_id) AS net_product_amount,
	(MAX(t1.price) + MAX(t1.freight_value)) * COUNT(DISTINCT t1.product_id) AS total_amount, -- included ship price
	MAX(t2.customer_id) AS customer_id, 
	MAX(t2.order_status) AS order_status, 
	MAX(t2.order_purchase_timestamp) AS order_purchase_timestamp, 
	MAX(t2.order_delivered_customer_date) AS order_delivered_customer_date
FROM {{ref('stg_order_items')}} t1
	INNER JOIN {{ref('stg_orders')}} t2 ON t1.order_id = t2.order_id
GROUP BY t1.order_id, t1.product_id
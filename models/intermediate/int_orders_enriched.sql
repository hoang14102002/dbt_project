SELECT 
	t1.customer_id, ISNULL(t2.customer_unique_id,t1.customer_id) AS customer_unique_id,
	t1.order_id, ISNULL(t2.customer_city,'Unknown') AS customer_city, 
	ISNULL(t2.customer_state,'Unknown') AS customer_state,
	t1.order_status, t1.order_purchase_timestamp, t1.order_delivered_customer_date
FROM {{ref('stg_orders')}} t1
	LEFT JOIN {{ref('stg_customers')}} t2 ON t2.customer_id = t1.customer_id
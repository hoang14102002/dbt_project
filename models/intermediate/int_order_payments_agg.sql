SELECT 
	order_id, payment_type, SUM(payment_value)  AS payment_value
FROM {{ref('stg_order_payments')}} 
GROUP BY order_id, payment_type
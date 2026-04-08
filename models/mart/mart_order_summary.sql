WITH voucher AS 
(
	SELECT order_id, SUM(payment_value) AS payment_value
	FROM {{ref('int_order_payments_agg')}} 
	WHERE payment_type = 'voucher'
	GROUP BY order_id
),
payment AS 
(
	SELECT order_id, SUM(payment_value) AS payment_value
	FROM {{ref('int_order_payments_agg')}} 
	WHERE payment_type <> 'voucher'
	GROUP BY order_id
),
review AS 
(
	SELECT order_id, AVG(review_score) AS review_score
	FROM {{ref('stg_order_reviews')}} 
	GROUP BY order_id
),
total_order AS 
(
	SELECT 
		order_id, COUNT(DISTINCT product_id) AS total_item,
		SUM(net_product_amount) AS net_product_amount,
		SUM(total_amount) AS total_amount,
		MAX(order_status) AS order_status,
		MAX(order_purchase_timestamp) AS order_purchase_timestamp,
		MAX(order_delivered_customer_date) AS order_delivered_customer_date
	FROM {{ref('int_order_details')}}
	GROUP BY order_id
)
	SELECT t1.order_id, t1.total_item,
		t1.net_product_amount AS total_product_value,
		ROUND((t1.total_amount - t1.net_product_amount),2) AS total_freight_value,
		t1.total_amount AS total_amount,
		ISNULL(-t2.payment_value,0) AS total_voucher_discount,
		ISNULL(t3.payment_value,0) AS total_payment,
		t1.order_status AS order_status,
		t1.order_purchase_timestamp AS order_purchase_timestamp,
		t1.order_delivered_customer_date AS order_delivered_customer_date,
		ISNULL(CAST(t4.review_score AS VARCHAR(15)),'Not review') AS review_score
	FROM total_order t1 
		LEFT JOIN voucher t2 ON t1.order_id = t2.order_id
		LEFT JOIN payment t3 ON t1.order_id = t3.order_id
		LEFT JOIN review t4 ON t1.order_id = t4.order_id

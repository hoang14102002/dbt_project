
WITH last_purchase_day AS 
(
	SELECT customer_unique_id, order_purchase_timestamp, customer_city, customer_state,
		ROW_NUMBER() OVER(PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp DESC) AS rn	
	FROM {{ref('int_orders_enriched')}}
),
temp AS 
(
	SELECT t1.customer_unique_id, 
		MAX(t3.customer_city) AS customer_city, MAX(t3.customer_state) AS customer_state,
		COUNT(DISTINCT t2.order_id) AS total_order,
		COUNT(DISTINCT t2.product_id) AS total_product,
		SUM(t2.total_amount) AS total_amount,
		SUM(t2.total_amount) - SUM(t2.freight_value) AS total_net_amount,
		ROUND((SUM(t2.total_amount) - SUM(t2.freight_value))/ COUNT(DISTINCT t2.order_id),2) AS avg_net_amount,
		MAX(t3.order_purchase_timestamp) AS last_purchase_date,
		DATEDIFF(DAY,MAX(t3.order_purchase_timestamp),GETDATE()) AS recency_days,
		NTILE(5) OVER (ORDER BY DATEDIFF(DAY,MAX(t3.order_purchase_timestamp),GETDATE())) AS recency_score,
		NTILE(5) OVER (ORDER BY COUNT(DISTINCT t2.order_id) DESC) AS frequency_score,
		NTILE(5) OVER (ORDER BY SUM(t2.total_amount) - SUM(t2.freight_value) DESC) AS monetary_score
	FROM {{ref('int_orders_enriched')}} t1
		INNER JOIN {{ref('int_order_details')}} t2 ON t1.order_id = t2.order_id
		LEFT JOIN last_purchase_day t3 ON t1.customer_unique_id = t3.customer_unique_id AND t3.rn = 1
	GROUP BY t1.customer_unique_id
)
	SELECT *,	
		CASE 
			WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'VIP'
			WHEN recency_score >= 3 AND frequency_score >= 3 THEN 'Loyal'
			WHEN recency_score = 5 THEN 'New Customer'
			WHEN recency_score <= 2 AND frequency_score >= 3 THEN 'At Risk'
			WHEN recency_score = 1 THEN 'Churned'
			ELSE 'Normal' END AS customer_segment
	FROM temp	
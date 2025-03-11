/* 1. Creating Cohorts */

/* 1.1 Cohorts for purpose of calculating retention and churn rates:
		- the month in which customer has made their last purchase will be counted in order to allign with the industry standards */

SELECT COUNT(DISTINCT customer_id)
FROM cohort_analysis_invoice_data
WHERE YEAR(invoice_date) <> "2010";

SELECT *
FROM cohort_analysis_invoice_data;

SELECT COUNT(DISTINCT customer_id)
FROM cohort_analysis_invoice_data;

DROP VIEW IF EXISTS cohort_retention_and_churn_info_view;

CREATE VIEW cohort_retention_and_churn_info_view AS
WITH customers_ordered_by_purchase_dates_cte AS (
	SELECT c.*, unit_price, ROUND(quantity * unit_price, 2) AS revenue_per_product_purchased
    FROM cohort_analysis_invoice_data c
    LEFT JOIN products p
	ON c.stock_code = p.stock_code
    AND DATE(c.invoice_date) = p.price_change_date
),
first_purchase_cte AS (
	SELECT customer_id, DATE(MIN(invoice_date)) AS first_purchase_date, ROUND(SUM(revenue_per_product_purchased), 2) AS revenue_per_customer
    FROM customers_ordered_by_purchase_dates_cte
    WHERE YEAR(invoice_date) <> "2010"  -- since the analysis is going to be focused on exclusively records from 2011, we're excluding those from 2010
    GROUP BY customer_id
),
last_purchase_cte AS (
	SELECT customer_id, DATE(MAX(invoice_date)) AS last_purchase_date
    FROM customers_ordered_by_purchase_dates_cte
    GROUP BY customer_id
),
cohorts_for_retention_cte AS (
	SELECT f.customer_id, first_purchase_date, DATE_FORMAT(first_purchase_date, '%Y-%m-01') AS beginning_of_month, 
    last_purchase_date, revenue_per_customer
	FROM first_purchase_cte f
    JOIN last_purchase_cte l
    ON f.customer_id = l.customer_id
),
months_between_first_and_last_purchase_cte AS (
	SELECT customer_id, beginning_of_month, DATE_FORMAT(beginning_of_month, '%Y-%m') as cohort_month, last_purchase_date,
		TIMESTAMPDIFF(MONTH, beginning_of_month, last_purchase_date) AS months_between_first_and_last_purchase, revenue_per_customer
	FROM cohorts_for_retention_cte
)
SELECT *
FROM months_between_first_and_last_purchase_cte
ORDER BY cohort_month;

SELECT 'customer_id', 'beginning_of_month', 'cohort_month', 'last_purchase_date', 'months_between_first_and_last_purchase', 
'revenue_per_customer'
UNION ALL
SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/purchase_info_per_customer.csv' 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM cohort_retention_and_churn_info_view; 

/* 2. Calculating Retention and Churn Rates and Customer LTV Per Cohort */

DROP VIEW IF EXISTS retention_and_churn_rates_view;

CREATE VIEW retention_and_churn_rates_view AS
WITH cohort_size_retention_cte AS (
	SELECT cohort_month, COUNT(customer_id) AS total_customers
	FROM cohort_retention_and_churn_info_view
    GROUP BY cohort_month
),
active_customers_per_cohort_cte AS (
	SELECT cohort_month, months_between_first_and_last_purchase, COUNT(customer_id) AS active_customers, 
    SUM(revenue_per_customer) AS cohort_revenue
    FROM cohort_retention_and_churn_info_view
    GROUP BY cohort_month, months_between_first_and_last_purchase
),
customer_lifetime_cte AS (
	SELECT cohort_month, 
		   COUNT(DISTINCT customer_id) AS unique_customers,
		   AVG(months_between_first_and_last_purchase) AS avg_customer_lifespan
	FROM cohort_retention_and_churn_info_view
	GROUP BY cohort_month
)
SELECT 
    a.cohort_month, 
    a.months_between_first_and_last_purchase,
    c.total_customers,
    a.active_customers,
	ROUND(a.cohort_revenue, 2) AS cohort_revenue,
    ROUND((a.cohort_revenue / c.total_customers), 2) AS avg_revenue_per_customer,
    ROUND(l.avg_customer_lifespan, 0) AS avg_customer_lifespan,
    ROUND((a.cohort_revenue / c.total_customers) * l.avg_customer_lifespan, 2) AS LTV,
	ROUND((a.active_customers / NULLIF(c.total_customers, 0)) * 100, 2) AS retention_rate,
    100 - ROUND((a.active_customers / NULLIF(c.total_customers, 0)) * 100, 2) AS churn_rate
FROM active_customers_per_cohort_cte a
JOIN cohort_size_retention_cte c 
ON a.cohort_month = c.cohort_month
JOIN customer_lifetime_cte l 
ON a.cohort_month = l.cohort_month
ORDER BY a.cohort_month, a.months_between_first_and_last_purchase;

SELECT SUM(active_customers)
FROM retention_and_churn_rates_view
WHERE cohort_month <> "2010-12";


/* Note: Customer LTV
	- since we have data available for only about a year this metric shouldn't be used for making long-term predictions, but it can still
      provide some value in a more short-term sense (like understanding customer profitability over their active period) */
      
SELECT 'cohort_month', 'months_between_first_and_last_purchase', 'total_customers', 'active_customers', 'cohort_revenue',
	'avg_revenue_per_customer', 'avg_customer_lifespan', 'LTV', 'retention_rate', 'churn_rate'
UNION ALL
SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/retention_churn_ltv_per_cohort.csv' 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM retention_and_churn_rates_view;      
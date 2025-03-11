/* DATA CLEANING AND PREPROCESSING */

/* customers_raw */

SELECT *
FROM customers_raw;

SELECT COUNT(*) AS total_count, COUNT(DISTINCT customer_id) AS unique_count
FROM customers_raw;  -- total count: 4376, distinct count: 4372 -- we have 4 duplicates 

/* Looking into these non-unique entries */

SELECT customer_id, COUNT(customer_id) AS count_of_entries
FROM customers_raw
GROUP BY customer_id
HAVING COUNT(customer_id) > 1;  -- identifying those ids that are showing up multiple times 


SELECT *
FROM customers_raw
WHERE customer_id IN (SELECT customer_id
							FROM customers_raw
							GROUP BY customer_id
							HAVING COUNT(customer_id) > 1)
ORDER BY customer_id;  -- pulling all information on those non-unique ids to make sure that besides customer_ids that are duplicate the same goes for the rest of the data

/* all 4 of them are duplicates so we'll proceed to removing them */

WITH duplicates_detection_cte AS (
	SELECT *, ROW_NUMBER() OVER(PARTITION BY customer_id, age, gender) AS row_num
	FROM customers_raw
)
SELECT *
FROM duplicates_detection_cte
WHERE row_num > 1;

DROP TABLE IF EXISTS customers;

CREATE TABLE `customers` (
  `customer_id` varchar(10) DEFAULT NULL,
  `age` int DEFAULT NULL,
  `gender` varchar(10) DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO customers
SELECT *, ROW_NUMBER() OVER(PARTITION BY customer_id, age, gender) AS row_num
FROM customers_raw;

SELECT *
FROM customers
WHERE row_num > 1;

DELETE FROM customers
WHERE row_num > 1;

ALTER TABLE customers
DROP COLUMN row_num;

/* ------------------------------------------------------------------------------------------------ */

SELECT *
FROM customers
WHERE age IS NULL  
OR age = '';        /* no null o missing values in age column */

SELECT *
FROM customers
WHERE gender IS NULL  
OR gender = '';     /* no null o missing values in gender column */

SELECT COUNT(*) AS count_of_missing_ids
FROM customers
WHERE customer_id IS NULL
OR customer_id = '';    /* no null or missing ids */


/* customers table is now clean and we'e ready for the next step */

/* ------------------------------------------------------------------------------------------------------------------------ */

/* products_raw */

SELECT *
FROM products_raw;

SELECT COUNT(*)
FROM products_raw;

/* Checking each column for NULL or missing values */

SELECT *
FROM products_raw
WHERE stock_code IS NULL  
OR stock_code = '';   -- no null or missing values in the stock_code column 

SELECT COUNT(*)
FROM products_raw
WHERE `description` IS NULL  
OR `description` = '';   -- 1443 rows with missing values in description column, but since this column isn't of value for the analysis we'll just replace them with NULLs 

SELECT *
FROM products_raw
WHERE price_change_date IS NULL;   -- there aren't any NULL values in price_change_date column 


SELECT *
FROM products_raw
WHERE unit_price IS NULL  
OR unit_price = '';     -- 2497 rows in unit_price column with missing values 

WITH products_without_price AS (
	SELECT *
	FROM products_raw
	WHERE unit_price IS NULL  
	OR unit_price = ''
)
SELECT *
FROM products_raw p
JOIN products_without_price pwp
ON p.stock_code = pwp.stock_code
AND p.price_change_date = pwp.price_change_date
AND p.unit_price <> 0;    /* 1664 rows that can be imputed, but we notice that for some dates there are multiple different prices for the same product stock_code 
							 because of the data limitation we don't know if there have been some discounts applied, so we'll assume that the highest price
                             on the day is the full price and we'll be using MAX function to extract those values and replace 0 */


DROP VIEW IF EXISTS prices_imputation1_view;

CREATE VIEW prices_imputation1_view AS
WITH products_without_price_cte AS (
    SELECT *
    FROM products_raw
    WHERE unit_price = 0
),
valid_prices_cte AS (
    SELECT 
        stock_code, 
        price_change_date, 
        MAX(unit_price) OVER (PARTITION BY stock_code, price_change_date) AS imputed_price
    FROM products_raw
    WHERE unit_price IS NOT NULL 
    AND unit_price <> ''
)
SELECT p.*, v.imputed_price
FROM products_without_price_cte p
LEFT JOIN valid_prices_cte v
ON p.stock_code = v.stock_code
AND p.price_change_date = v.price_change_date;

UPDATE products_raw p
JOIN prices_imputation1_view v
ON p.stock_code = v.stock_code
AND p.price_change_date = v.price_change_date
SET p.unit_price = v.imputed_price
WHERE p.unit_price = 0;

SELECT *
FROM products_raw
WHERE unit_price IS NULL  
OR unit_price = '';  -- number of rows with missing unit_price is 1642 

/* The rest of the missing values will be imputed with the highest price for the same stock_code  */

DROP VIEW IF EXISTS prices_imputation2_view;

CREATE VIEW prices_imputation2_view AS
WITH price_updates AS (
    SELECT stock_code, price_change_date, unit_price, 
           NULLIF(unit_price, 0) AS cleaned_price -- Convert 0 to NULL for imputation
    FROM products_raw
),
filled_prices AS (
    SELECT stock_code, price_change_date, unit_price,
           COALESCE(
               cleaned_price,
               MAX(cleaned_price) OVER (
                   PARTITION BY stock_code 
                   ORDER BY price_change_date 
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) 
           ) AS imputed_price
    FROM price_updates
)
SELECT stock_code, price_change_date, imputed_price
FROM filled_prices;  -- 223 rows remain having a unit_price 0 

SELECT *
FROM prices_imputation2_view
WHERE imputed_price IS NULL;  -- some stock_codes: 10123G, 10134, 16053, 16162M 

SELECT *
FROM products_raw
WHERE stock_code = '16162M';  /* after checking on a couple random stock_codes from above that we weren't able to impute, I see that 
								 for most of them we have never had price information, they ususaly show up only once in our products table,
                                 but there are instances, as with stock_code = '16162M', where there are other prices, but the NULL remained
                                 for those earliest date in the table since we couldn't look for an earlier date for the non-NULL value.
                                 In order to impute as much as possible, we'll now do the same we did above, but we'll look for the first next
                                 available price and use that to impute some of these NULLs */
                                 
UPDATE products_raw  p
JOIN prices_imputation2_view v
ON p.stock_code = v.stock_code
AND p.price_change_date = v.price_change_date
SET p.unit_price = v.imputed_price
WHERE p.unit_price IS NULL;

SELECT *
FROM products_raw
WHERE unit_price IS NULL;  -- 223 rows still contain NULLs 

DROP VIEW IF EXISTS prices_imputation3_view;

CREATE VIEW prices_imputation3_view AS
WITH price_updates AS (
    SELECT stock_code, price_change_date, unit_price, 
           NULLIF(unit_price, 0) AS cleaned_price -- Convert 0 to NULL for imputation
    FROM products_raw
),
filled_prices AS (
    SELECT stock_code, price_change_date, unit_price,
           COALESCE(
               cleaned_price,
               MAX(cleaned_price) OVER (
                   PARTITION BY stock_code 
                   ORDER BY price_change_date 
                   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
               ) 
           ) AS imputed_price
    FROM price_updates
)
SELECT stock_code, price_change_date, imputed_price
FROM filled_prices;  

SELECT *
FROM prices_imputation3_view
WHERE imputed_price IS NULL;  /* some stock_codes: 10123G, 10134, 16053, 20689, 23595, 84611B, PADS (total 136)
								 Since all of these appear only once and there's only 136 of them, we'll leave them as NULLs since these also
                                 could be some wrong entires and there really isn't a good way to impute them without having any sort of 
                                 historical data */ 



SELECT stock_code, count(stock_code)
FROM prices_imputation3_view
WHERE imputed_price IS NULL
GROUP BY stock_code;


UPDATE products_raw  p
JOIN prices_imputation3_view v
ON p.stock_code = v.stock_code
AND p.price_change_date = v.price_change_date
SET p.unit_price = v.imputed_price
WHERE p.unit_price IS NULL;

SELECT *
FROM products_raw
WHERE stock_code IN ('84611B', 'PADS')
ORDER BY stock_code; 


/* Checking for duplicates and dealing with multiple unit_price for products with the same date and stock_code
	- Since these 3 tables were derived from one original table where each product from the order was being stored individually, 
      when I created products table, I had instances where the product with the same stock_code and date had a different unit_price. 
      To make sure this dataset is as optimal as possible for the purpose of my project, and so that I don't over- or under- estimate the 
      revenue, I decided to calculate an average unit_price for each combination of variables stock_code and date and use that value */

SELECT COUNT(*)
FROM products_raw;

CREATE VIEW average_prices_view AS
WITH average_price_cte AS (
	SELECT *, 
		AVG(unit_price) OVER(PARTITION BY stock_code, price_change_date) AS avg_price_per_product_per_date,
		ROW_NUMBER() OVER(PARTITION BY stock_code, price_change_date) AS row_num
	FROM products_raw
)
SELECT stock_code, price_change_date, unit_price, ROUND(avg_price_per_product_per_date, 2) AS unit_price_2, row_num
FROM average_price_cte
ORDER BY stock_code, price_change_date;


SELECT *
FROM average_prices_view
/* WHERE row_num > 1*/
ORDER BY stock_code, price_change_date;

/* Updating table products_raw - keeping unit_price_2 as unit_price */

UPDATE products_raw  p
JOIN average_prices_view v
ON p.stock_code = v.stock_code
AND p.price_change_date = v.price_change_date
SET p.unit_price = v.unit_price_2;

SELECT * 
FROM products_raw
ORDER BY stock_code, price_change_date;

DROP TABLE IF EXISTS products;

CREATE TABLE `products` (
  `stock_code` varchar(15) DEFAULT NULL,
  `description` varchar(100) DEFAULT NULL,
  `price_change_date` date DEFAULT NULL,
  `unit_price` float DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO products
SELECT *, ROW_NUMBER() OVER(PARTITION BY stock_code, price_change_date) AS row_num
FROM products_raw;

SELECT * 
FROM products
ORDER BY stock_code, price_change_date;

SELECT *
FROM products
WHERE row_num > 1;

DELETE FROM products
WHERE row_num > 1;

ALTER TABLE products
DROP COLUMN row_num;


SELECT stock_code, price_change_date, unit_price
FROM products
ORDER BY stock_code, price_change_date;


SELECT *
FROM products
WHERE unit_price < 0;    -- one product with negative value for unit_price and we'll set it to null since in the query below we weren't
						    -- able to find an entry with correct unit_price for this stock_code

SELECT *
FROM products_raw
WHERE stock_code = 'B';

UPDATE products
SET unit_price = NULL 
WHERE unit_price < 0;

SELECT *
FROM products
WHERE unit_price IS NULL;

/* invoices_raw 
	- I'm noticing that all product purchased by the same customer and at the same time have the same invoice_no, so if we group by invoice_no
      we'll be getting to an actual orders */

SELECT *
FROM invoices_raw;

SELECT COUNT(*)
FROM invoices_raw;

/* Checking columns for NULL and missing values */

SELECT *
FROM invoices_raw
WHERE invoice_no IS NULL
OR invoice_no = '';   /* no null or missing values in invoice_no */

SELECT *
FROM invoices_raw
WHERE stock_code IS NULL
OR stock_code = '';   /* no null or missing values in stock_code */

SELECT *
FROM invoices_raw
WHERE customer_id IS NULL
OR customer_id = '';  

SELECT COUNT(*)
FROM invoices_raw
WHERE customer_id IS NULL
OR customer_id = '';   /* 135080 missing values in customer_id column
						  Since we won't be able to group these customers without ids into correct cohorts, but the data is still valuable
                          for calculating revenue, total quantities, etc we'll later create a separate table or a view that we'll use for
                          cohort analysis with removed rows with missing customer_id, and keep the original for other purposes*/
                          
SELECT *
FROM invoices_raw
WHERE invoice_date IS NULL;     -- no null or missing values in invoice_date 

SELECT *
FROM invoices_raw
WHERE quantity IS NULL
OR quantity = '';     -- no null or missing values in quantity 

/* Quantity: negative vealies and some very large ones */

SELECT *
FROM invoices_raw
ORDER BY quantity;  -- we have negative values in quantity 

SELECT COUNT(*)
FROM invoices_raw
WHERE quantity < 0;  -- we have 10624 negative values in quantity 

SELECT MIN(quantity), MAX(quantity)
FROM invoices_raw;  -- quantities range between -80995 and 80995 

SELECT *
FROM invoices_raw
ORDER BY quantity DESC;  -- we have 3 very high quantity values: 80995, 74215, 12540 (stock_codes are 23843, 23166, 84826); 
						 -- the rest fall below 5600 units
                            
SELECT *
FROM invoices_raw
WHERE stock_code IN (23843, 23166, 84826)
ORDER BY stock_code;   -- I'm noticing that orders with quantities 80995 and 74215 are immediately followed by orders with quantities -80995 and -74215
					   -- This is telling me that these were mistakes because they positive and negative value ones have the same stock_code and invoice_date, happened minutes apart
                       -- The one with quantity 12540 seems to be a true order
                       -- I wonder if those invoices with negative quantities are actually returns
                       -- I'm noticing that in case of what I think are returns, returns are stored under new invoice_no
                       
-- Are invoices with negative quantities are actually returns?

CREATE VIEW returns_view AS
WITH invoices_with_negative_quantity_cte AS (
	SELECT *, ABS(quantity) AS absolute_quantity
	FROM invoices_raw
	HAVING quantity < 0
)
SELECT i.*, ct.invoice_no AS return_invoice_no, ct.stock_code AS return_stock_code, ct.quantity AS return_quantity, ct.invoice_date AS return_invoice_date, ct.customer_id AS return_customer_id, ct.country AS return_country, ct.absolute_quantity
FROM invoices_raw i
JOIN invoices_with_negative_quantity_cte ct   -- when checking if an order is a return a couple things need to match
ON i.stock_code = ct.stock_code     				-- stock code
AND i.customer_id = ct.customer_id  				-- customer_id
AND i.quantity = ct.absolute_quantity				-- most likely the date portion of the timestamp (I noticed that they seem to happen minutes apart)
AND DATE(i.invoice_date) = DATE(ct.invoice_date);

-- All these 2222 entries should be removed                        

CREATE VIEW return_invoices AS
SELECT invoice_no, stock_code, quantity, invoice_date, customer_id, country
FROM returns_view
UNION ALL
SELECT return_invoice_no, return_stock_code, return_quantity, return_invoice_date, return_customer_id, return_country
FROM returns_view;       

DROP TABLE IF EXISTS invoices;
CREATE TABLE invoices
LIKE invoices_raw;

INSERT invoices
SELECT * 
FROM invoices_raw;

-- Before deleting anything I want to look for duplicates


WITH duplicates_cte AS (
	SELECT *, 
		ROW_NUMBER() OVER(PARTITION BY invoice_no, stock_code, quantity, invoice_date, customer_id, country) AS row_num
	FROM invoices
)
SELECT *
FROM duplicates_cte
WHERE row_num > 1;      -- there are 5429 duplicates and we'll remove them first

CREATE TABLE `invoices2` (
  `invoice_no` varchar(15) DEFAULT NULL,
  `stock_code` varchar(15) DEFAULT NULL,
  `quantity` int DEFAULT NULL,
  `invoice_date` datetime DEFAULT NULL,
  `customer_id` varchar(10) DEFAULT NULL,
  `country` varchar(30) DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO invoices2
SELECT *, ROW_NUMBER() OVER(PARTITION BY invoice_no, stock_code, quantity, invoice_date, customer_id, country) AS row_num
FROM invoices;

DELETE FROM invoices2
WHERE row_num > 1;

ALTER TABLE invoices2
DROP COLUMN row_num;

-- Back to looking into quantities


WITH delete_flag_cte AS (
	SELECT i.*, 
		CASE WHEN r.invoice_no IS NOT NULL THEN 'delete'
		ELSE 'keep'
		END AS delete_flag
	FROM invoices2 i
	LEFT JOIN return_invoices r
	ON i.invoice_no = r.invoice_no
	AND i.stock_code = r.stock_code
	AND i.quantity = r.quantity
	AND i.invoice_date = r.invoice_date
	AND i.customer_id = r.customer_id
)
SELECT *
FROM delete_flag_cte
/*WHERE delete_flag = 'delete'*/
ORDER BY stock_code;

DROP TABLE IF EXISTS invoices3;
CREATE TABLE `invoices3` (
  `invoice_no` varchar(15) DEFAULT NULL,
  `stock_code` varchar(15) DEFAULT NULL,
  `quantity` int DEFAULT NULL,
  `invoice_date` datetime DEFAULT NULL,
  `customer_id` varchar(10) DEFAULT NULL,
  `country` varchar(30) DEFAULT NULL,
  `delete_flag` varchar(10)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO invoices3
SELECT i.invoice_no, i.stock_code, i.quantity, i.invoice_date, i.customer_id, i.country, 
	CASE WHEN r.invoice_no IS NOT NULL THEN 'delete'
		ELSE 'keep'
		END AS delete_flag
FROM invoices2 i
LEFT JOIN return_invoices r
ON i.invoice_no = r.invoice_no
AND i.stock_code = r.stock_code
AND i.quantity = r.quantity
AND i.invoice_date = r.invoice_date
AND i.customer_id = r.customer_id;

DELETE FROM invoices3
WHERE delete_flag = 'delete';

SELECT *
FROM invoices3
WHERE delete_flag = 'delete';

ALTER TABLE invoices3
DROP COLUMN delete_flag;

-- Let's look at the remainder of invoices with negative quantities

SELECT *
FROM invoices3
WHERE quantity < 0;   -- 9650 such entries

DROP VIEW IF EXISTS returns_u_to_30_days;

CREATE VIEW returns_up_to_30_days AS
WITH invoices_with_negative_quantity_cte AS (
	SELECT *, ABS(quantity) AS absolute_quantity
	FROM invoices3
	WHERE quantity < 0
),
days_between_purchase_and_return_cte AS (
	SELECT i.*, TIMESTAMPDIFF(DAY, i.invoice_date, ct.invoice_date) AS days_between_purchase_and_return, ct.invoice_no AS return_invoice_no, 
		ct.stock_code AS return_stock_code, ct.quantity AS return_quantity, ct.invoice_date AS return_invoice_date, ct.customer_id AS return_customer_id, 
        ct.country AS return_country, ct.absolute_quantity
	FROM invoices3 i
	JOIN invoices_with_negative_quantity_cte ct   -- when checking if an order is a return a couple things need to match
	ON i.stock_code = ct.stock_code     				-- stock code
	AND i.customer_id = ct.customer_id  				-- customer_id
	AND i.quantity = ct.absolute_quantity  -- if we allow the dates do be different, we get 8324 returned orders which could be the case since there's usualy a period of time during which a customer can return their product
)
SELECT * 
FROM days_between_purchase_and_return_cte
WHERE days_between_purchase_and_return <= 30
AND  days_between_purchase_and_return > 0
ORDER BY days_between_purchase_and_return;   -- if we set return window to 30 days, we get 1938 orders that were returns



DROP VIEW IF EXISTS return_invoices_up_to_30_days_view;

CREATE VIEW return_invoices_up_to_30_days_view AS
SELECT invoice_no, stock_code, quantity, invoice_date, customer_id, country
FROM returns_up_to_30_days
UNION ALL
SELECT return_invoice_no, return_stock_code, return_quantity, return_invoice_date, return_customer_id, return_country
FROM returns_up_to_30_days;

WITH delete_flag_cte AS (
	SELECT i.*, 
		CASE WHEN r.invoice_no IS NOT NULL THEN 'delete'
		ELSE 'keep'
		END AS delete_flag
	FROM invoices3 i
	LEFT JOIN return_invoices_up_to_30_days_view r
	ON i.invoice_no = r.invoice_no
	AND i.stock_code = r.stock_code
	AND i.quantity = r.quantity
	AND i.invoice_date = r.invoice_date
	AND i.customer_id = r.customer_id
)
SELECT *
FROM delete_flag_cte
WHERE delete_flag = 'delete'
ORDER BY stock_code;

DROP TABLE IF EXISTS invoices4;
CREATE TABLE `invoices4` (
  `invoice_no` varchar(15) DEFAULT NULL,
  `stock_code` varchar(15) DEFAULT NULL,
  `quantity` int DEFAULT NULL,
  `invoice_date` datetime DEFAULT NULL,
  `customer_id` varchar(10) DEFAULT NULL,
  `country` varchar(30) DEFAULT NULL,
  `delete_flag` varchar(10)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO invoices4
SELECT i.invoice_no, i.stock_code, i.quantity, i.invoice_date, i.customer_id, i.country, 
	CASE WHEN r.invoice_no IS NOT NULL THEN 'delete'
		ELSE 'keep'
		END AS delete_flag
FROM invoices3 i
LEFT JOIN return_invoices_up_to_30_days_view r
ON i.invoice_no = r.invoice_no
AND i.stock_code = r.stock_code
AND i.quantity = r.quantity
AND i.invoice_date = r.invoice_date
AND i.customer_id = r.customer_id;

DELETE FROM invoices4
WHERE delete_flag = 'delete';

SELECT *
FROM invoices4
WHERE delete_flag = 'delete';

ALTER TABLE invoices4
DROP COLUMN delete_flag;

SELECT *
FROM invoices4
WHERE quantity < 0;  -- 7552 rows left

-- For remained 7552 rows holding negative values in column quantity we'll apply this strategy:
	-- In order to keep as much as data as possible, we'll try replacing them with average quantity sold per stock_code
    -- If it so happens that we're not able ro replace all of them (because the buesiness has never properly stored quantity sold for the
    -- particular stock_code, we'll have to remove those rows
    
UPDATE invoices4 AS t1
JOIN (
    SELECT stock_code, ROUND(AVG(quantity), 0) AS avg_quantity
    FROM invoices_raw
    WHERE quantity > 0
    GROUP BY stock_code
) AS t2
ON t1.stock_code = t2.stock_code
SET t1.quantity = t2.avg_quantity
WHERE t1.quantity < 0 AND t2.avg_quantity IS NOT NULL;

SELECT *
FROM invoices4
WHERE quantity < 0;  -- 215 rows left which we'll now delete

DELETE FROM invoices4
WHERE quantity < 0;

SELECT *
FROM invoices4;

/* Looking into column country */

SELECT *
FROM invoices4
WHERE country = ''
OR country  IS NULL;  -- no missing values here

/* Looking into invoices made by customers with missin ids */

SELECT COUNT(*)
FROM invoices4
WHERE customer_id IS NULL
OR customer_id = '';    -- 134307 missing values in customer_id column


WITH total_stats AS (
    SELECT 
        COUNT(*) AS total_orders,
        ROUND(SUM(unit_price * quantity),0) AS total_revenue,
        COUNT(DISTINCT i.stock_code) AS total_unique_products_sold
    FROM invoices4 i
    LEFT JOIN products p
    ON i.stock_code = p.stock_code
    AND DATE(i.invoice_date) = p.price_change_date
),
missing_customer_stats AS (
    SELECT 
        COUNT(*) AS missing_orders,
        ROUND(SUM(unit_price * quantity),0) AS missing_revenue,
        COUNT(DISTINCT i.stock_code) AS missing_unique_products
    FROM invoices4 i
    LEFT JOIN products p
    ON i.stock_code = p.stock_code
    AND DATE(i.invoice_date) = p.price_change_date
    WHERE customer_id = ''
)
SELECT 
    ROUND((missing_orders * 100.0 / total_orders),0) AS percent_missing_orders,
    ROUND((missing_revenue * 100.0 / total_revenue),0) AS percent_missing_revenue,
    ROUND((missing_unique_products * 100.0 / total_unique_products_sold),0) AS percent_missing_unique_products
FROM total_stats, missing_customer_stats;


-- Results summary:
	-- 25% of invoices have missing customer_id
    -- 17% of total generated revenue has been generated through orders with missing customer_id
    -- 93% of total unique products sold are within orders with missing customer_id
    
-- Next steps:
	-- Since cohort analysis relies on tracking repeat purchases over time, orders without customer_id can't be grouped into cohorts, so
	-- we'll create a view where we'll store all invoices with information about customer_id
    -- Since 17% of total revenue comes from these orders, ignoring them completely would skew overall revenue analysis and keeping them in 
    -- the original table means we can still analyze:
			-- Total revenue, product sales, average order value
			-- Popular products (93% of unique products appear in these orders!)
			-- Seasonal trends, sales spikes, and overall business performance 

SELECT *
FROM invoices4;

SELECT 'invoice_no', 'stock_code', 'quantity', 'invoice_date', 'customer_id', 'country'
UNION ALL
SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/all_invoices_data.csv' 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM invoices4; 

DROP VIEW IF EXISTS cohort_analysis_invoice_data;

CREATE VIEW cohort_analysis_invoice_data AS
SELECT *
FROM invoices4
WHERE NULLIF(customer_id,'') IS NOT NULL;

SELECT *
FROM cohort_analysis_invoice_data;

SELECT 'invoice_no', 'stock_code', 'quantity', 'invoice_date', 'customer_id', 'country'
UNION ALL
SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cohort_analysis_invoice_data.csv' 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM cohort_analysis_invoice_data; 

SELECT *
FROM products;

SELECT 'stock_code', 'description', 'price_change_date', 'unit_price'
UNION ALL
SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/products.csv' 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM products; 

SELECT *
FROM customers;

SELECT 'customer_id', 'age', 'gender' 
UNION ALL
SELECT * 
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/customers.csv' 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
FROM customers; 




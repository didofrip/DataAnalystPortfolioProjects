/*
===============================================================================
ETL Pipeline & Data Modeling Script
Project: Olist E-Commerce Analytics
Author: Yaroslav Pryimak
Description: 
    This script performs the complete ETL process:
    1. Creates the database and Staging tables (Raw Layer).
    2. Loads data from CSV files.
    3. Transforms and cleans data into Dimensions and Facts (Star Schema).
    4. Handles data quality issues (duplicates, nulls, orphans).
    5. Establishes Referential Integrity via Foreign Keys.
===============================================================================
*/

-- ============================================================================
-- STEP 1: DATABASE INITIALIZATION
-- ============================================================================
CREATE DATABASE Olist_Store;

USE Olist_Store;

-- Check secure_file_priv to ensure we can load files from the specific directory
SHOW VARIABLES LIKE "secure_file_priv";


-- ============================================================================
-- STEP 2: STAGING LAYER (RAW DATA INGESTION)
-- Strategy: Use TEXT data types to prevent load errors, then clean in next steps.
-- ============================================================================

-- 2.1 Raw Customers
CREATE TABLE raw_customers (
	customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/olist_customers_dataset.csv' 
INTO TABLE raw_customers 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM raw_customers;

-- 2.2 Raw Geolocation
CREATE TABLE raw_geolocation (
	geolocation_zip_code_prefix TEXT,
    geolocation_lat TEXT,
    geolocation_lng TEXT,
    geolocation_city TEXT,
    geolocation_state TEXT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/olist_geolocation_dataset.csv' 
INTO TABLE raw_geolocation 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM raw_geolocation;

-- 2.3 Raw Order Items
CREATE TABLE raw_order_items (
	order_id TEXT,
    order_item_id TEXT,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TEXT,
    price TEXT,
    freight_value TEXT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/olist_order_items_dataset.csv' 
INTO TABLE raw_order_items 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM raw_order_items;

-- 2.4 Raw Payments
CREATE TABLE raw_order_payments (
	order_id TEXT,
    payment_sequential TEXT,
    payment_type TEXT,
    payment_installments TEXT,
    payment_value TEXT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/olist_order_payments_dataset.csv' 
INTO TABLE raw_order_payments 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM raw_order_payments;

-- 2.5 Raw Reviews
CREATE TABLE raw_order_reviews (
	review_id TEXT,
    order_id TEXT,
    review_score TEXT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TEXT,
    review_answer_timestamp TEXT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/olist_order_reviews_dataset.csv' 
INTO TABLE raw_order_reviews 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM raw_order_reviews;

-- 2.6 Raw Orders
CREATE TABLE raw_orders (
	order_id TEXT,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TEXT,
    order_approved_at TEXT,
    order_delivered_carrier_date TEXT,
    order_delivered_customer_date TEXT,
    order_estimated_delivery_date TEXT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/olist_orders_dataset.csv' 
INTO TABLE raw_orders 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM raw_orders;

-- 2.7 Raw Products
CREATE TABLE raw_products (
	product_id TEXT,
    product_category_name TEXT,
    product_name_lenght TEXT,
    product_description_lenght TEXT,
    product_photos_qty TEXT,
    product_weight_g TEXT,
    product_length_cm TEXT,
    product_height_cm TEXT,
    product_width_cm TEXT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/olist_products_dataset.csv' 
INTO TABLE raw_products 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM raw_products;

-- 2.8 Raw Sellers
CREATE TABLE raw_sellers (
	seller_id TEXT,
    seller_zip_code_prefix TEXT,
    seller_city TEXT,
    seller_state TEXT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/olist_sellers_dataset.csv' 
INTO TABLE raw_sellers 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM raw_sellers;

-- 2.9 Translation Table
CREATE TABLE raw_product_category_name_translation (
	product_category_name TEXT,
    product_category_name_english TEXT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/product_category_name_translation.csv' 
INTO TABLE raw_product_category_name_translation 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM raw_product_category_name_translation;


----------------------------------------------------------------------------------------------------------------------------


SHOW CREATE DATABASE Olist_Store;

-- ============================================================================
-- STEP 3: DIMENSION LAYER (CLEANING & TRANSFORMATION)
-- Goal: Create clean lookup tables with correct data types.
-- ============================================================================

-- 3.1 Dim Products
-- Logic: Join with translation table to get English names, cast numerical values.
DROP TABLE IF EXISTS dim_products;

CREATE TABLE dim_products (
    PRIMARY KEY (product_id) 
) AS 
SELECT 
    CAST(o.product_id AS CHAR(32)) AS product_id,
    -- Replace Portuguese category names with English ones
    COALESCE(t.product_category_name_english, NULLIF(TRIM(o.product_category_name), '')) AS product_category_name, 
    CAST(NULLIF(TRIM(REPLACE(o.product_name_lenght, '"', '')), '') AS UNSIGNED) AS product_name_lenght,
    CAST(NULLIF(TRIM(REPLACE(o.product_description_lenght, '"', '')), '') AS UNSIGNED) AS product_description_lenght,
    CAST(NULLIF(TRIM(REPLACE(o.product_photos_qty, '"', '')), '') AS UNSIGNED) AS product_photos_qty,
    CAST(NULLIF(TRIM(REPLACE(o.product_weight_g, '"', '')), '') AS UNSIGNED) AS product_weight_g,
    CAST(NULLIF(TRIM(REPLACE(o.product_length_cm, '"', '')), '') AS UNSIGNED) AS product_length_cm,
    CAST(NULLIF(TRIM(REPLACE(o.product_height_cm, '"', '')), '') AS UNSIGNED) AS product_height_cm,
    CAST(NULLIF(TRIM(REPLACE(o.product_width_cm, '"', '')), '') AS UNSIGNED) AS product_width_cm
FROM raw_products o
LEFT JOIN raw_product_category_name_translation t
    ON o.product_category_name = t.product_category_name;

SELECT * FROM dim_products;

DESC dim_products;

----------------------------------------------------------------------------------------------------------------------------

SELECT * FROM raw_customers;

-- 3.2 Dim Customers
DROP TABLE IF EXISTS dim_customers;

CREATE TABLE dim_customers (
    PRIMARY KEY (customer_id) 
) AS 
SELECT 
	CAST(customer_id AS CHAR(32)) AS customer_id,
    CAST(customer_unique_id AS CHAR(32)) AS customer_unique_id,
    CAST(NULLIF(TRIM(REPLACE(customer_zip_code_prefix, '"', '')), '') AS CHAR(5)) AS customer_zip_code_prefix,
    NULLIF(TRIM(customer_city), '') AS customer_city,
    CAST(customer_state AS CHAR(2)) AS customer_state
FROM raw_customers;

SELECT * FROM dim_customers;

DESC dim_customers;

----------------------------------------------------------------------------------------------------------------------------

SELECT * FROM raw_geolocation;

-- 3.3 Dim Geolocation
-- Logic: Aggregate duplicate Zip Codes by taking the average Latitude/Longitude
-- to ensure the Zip Code can be used as a Primary Key.
DROP TABLE IF EXISTS dim_geolocation;

CREATE TABLE dim_geolocation (
    PRIMARY KEY (geolocation_zip_code_prefix) 
) AS 
SELECT 
	CAST(geolocation_zip_code_prefix AS CHAR(5)) AS geolocation_zip_code_prefix,
	AVG(CAST(NULLIF(TRIM(geolocation_lat), '') AS DOUBLE)) AS geolocation_lat,
    AVG(CAST(NULLIF(TRIM(geolocation_lng), '') AS DOUBLE)) AS geolocation_lng,
    MAX(TRIM(geolocation_city)) AS geolocation_city,
	MAX(CAST(geolocation_state AS CHAR(2))) AS geolocation_state
FROM raw_geolocation
GROUP BY raw_geolocation.geolocation_zip_code_prefix;

SELECT * FROM dim_geolocation;

----------------------------------------------------------------------------------------------------------------------------

SELECT * FROM raw_sellers;

-- 3.4 Dim Sellers
DROP TABLE IF EXISTS dim_sellers;

CREATE TABLE dim_sellers (
	PRIMARY KEY (seller_id)
) AS 
SELECT 
	CAST(seller_id AS CHAR(32)) AS seller_id,
	CAST(NULLIF(TRIM(REPLACE(seller_zip_code_prefix, '"', '')), '') AS CHAR(5)) AS seller_zip_code_prefix,
    TRIM(seller_city) AS seller_city,
    CAST(seller_state AS CHAR(2))AS seller_state
FROM raw_sellers;

SELECT * FROM dim_sellers;

----------------------------------------------------------------------------------------------------------------------------

SELECT * FROM raw_orders;

-- ============================================================================
-- STEP 4: FACT LAYER (TRANSACTIONAL DATA)
-- Goal: Parse dates, clean strings, and ensure correct numeric types for metrics.
-- ============================================================================

-- 4.1 Fact Orders
DROP TABLE IF EXISTS fact_orders;

CREATE TABLE fact_orders (
    PRIMARY KEY (order_id)
) AS 
SELECT 
    CAST(order_id AS CHAR(32)) AS order_id,
    CAST(customer_id AS CHAR(32)) AS customer_id,
    TRIM(order_status) AS order_status,
    STR_TO_DATE(NULLIF(TRIM(REPLACE(order_purchase_timestamp, '"', '')), ''), '%Y-%m-%d %H:%i:%s') AS order_purchase_timestamp,
    STR_TO_DATE(NULLIF(TRIM(REPLACE(order_approved_at, '"', '')), ''), '%Y-%m-%d %H:%i:%s') AS order_approved_at,
    STR_TO_DATE(NULLIF(TRIM(REPLACE(order_delivered_carrier_date, '"', '')), ''), '%Y-%m-%d %H:%i:%s') AS order_delivered_carrier_date,
    STR_TO_DATE(NULLIF(TRIM(REPLACE(order_delivered_customer_date, '"', '')), ''), '%Y-%m-%d %H:%i:%s') AS order_delivered_customer_date,
    STR_TO_DATE(NULLIF(TRIM(REPLACE(order_estimated_delivery_date, '"', '')), ''), '%Y-%m-%d %H:%i:%s') AS order_estimated_delivery_date

FROM raw_orders;

SELECT * FROM fact_orders;

----------------------------------------------------------------------------------------------------------------------------

SELECT * FROM raw_order_items;

-- 4.2 Fact Order Items
-- Note: Uses a composite primary key (order_id + order_item_id)
DROP TABLE IF EXISTS fact_order_items;

CREATE TABLE fact_order_items(
	PRIMARY KEY (order_id, order_item_id)
) AS 
SELECT 
	CAST(order_id AS CHAR(32)) AS order_id,
    CAST(NULLIF(REPLACE(TRIM(order_item_id), '"', ''), '') AS UNSIGNED) AS order_item_id,
    CAST(product_id AS CHAR(32)) AS product_id,
    CAST(seller_id AS CHAR(32)) AS seller_id,
    STR_TO_DATE(NULLIF(TRIM(REPLACE(shipping_limit_date, '"', '')), ''), '%Y-%m-%d %H:%i:%s') AS shipping_limit_date,
    CAST(NULLIF(REPLACE(TRIM(price), '"', ''), '') AS DECIMAL(10, 2)) AS price,
    CAST(NULLIF(REPLACE(TRIM(freight_value), '"', ''), '') AS DECIMAL(10, 2)) AS freight_value
FROM raw_order_items;

SELECT * FROM fact_order_items;

----------------------------------------------------------------------------------------------------------------------------

SELECT * FROM raw_order_payments;

-- 4.3 Fact Order Payments
-- Note: Uses a composite primary key (order_id + payment_sequential)
DROP TABLE IF EXISTS fact_order_payments;

CREATE TABLE fact_order_payments(
	PRIMARY KEY (order_id, payment_sequential)
) AS 
SELECT 
	CAST(order_id AS CHAR(32)) AS order_id,
    CAST(NULLIF(REPLACE(TRIM(payment_sequential), '"', ''), '') AS UNSIGNED) AS payment_sequential,
    TRIM(payment_type) AS payment_type,
    CAST(NULLIF(REPLACE(TRIM(payment_installments), '"', ''), '') AS UNSIGNED) AS payment_installments,
    CAST(NULLIF(REPLACE(TRIM(payment_value), '"', ''), '') AS DECIMAL(10, 2)) AS payment_value
FROM raw_order_payments;

SELECT * FROM fact_order_payments;

----------------------------------------------------------------------------------------------------------------------------

SELECT * FROM raw_order_reviews;

-- 4.4 Fact Order Reviews
-- Note: Duplicate handling logic included via GROUP BY (if needed in future)
DROP TABLE IF EXISTS fact_order_reviews;

CREATE TABLE fact_order_reviews(
	PRIMARY KEY (review_id, order_id)
) AS 
SELECT 
	CAST(review_id AS CHAR(32)) AS review_id,
    CAST(order_id AS CHAR(32)) AS order_id,
    CAST(review_score AS UNSIGNED) AS review_score,
    NULLIF(REPLACE(TRIM(review_comment_title), '"', ''), '') AS review_comment_title,
    NULLIF(REPLACE(TRIM(review_comment_message), '"', ''), '') AS review_comment_message,
    STR_TO_DATE(NULLIF(TRIM(REPLACE(review_creation_date, '"', '')), ''), '%Y-%m-%d %H:%i:%s') AS review_creation_date,
    STR_TO_DATE(NULLIF(TRIM(REPLACE(review_answer_timestamp, '"', '')), ''), '%Y-%m-%d %H:%i:%s') AS review_answer_timestamp
FROM raw_order_reviews;

SELECT * FROM fact_order_reviews;

----------------------------------------------------------------------------------------------------------------------------

-- ============================================================================
-- STEP 5: DATA QUALITY & REFERENTIAL INTEGRITY CHECKS
-- Goal: Remove "orphan" records to ensure all Foreign Keys can be established.
-- ============================================================================

SELECT count(*) 
FROM fact_orders f
LEFT JOIN dim_customers d 
	ON f.customer_id = d.customer_id
WHERE d.customer_id IS NULL;

-- 5.1 Identify and delete orders without valid customers
DELETE FROM fact_orders 
WHERE customer_id NOT IN (SELECT customer_id FROM dim_customers);

-- 5.2 Clean Fact Items (must map to Orders, Products, and Sellers)
DELETE FROM fact_order_items 
WHERE order_id NOT IN (SELECT order_id FROM fact_orders);

DELETE FROM fact_order_items 
WHERE product_id NOT IN (SELECT product_id FROM dim_products);

DELETE FROM fact_order_items 
WHERE seller_id NOT IN (SELECT seller_id FROM dim_sellers);

-- 5.3 Clean Payments (must map to Orders)
DELETE FROM fact_order_payments 
WHERE order_id NOT IN (SELECT order_id FROM fact_orders);

-- 5.4 Clean Reviews (must map to Orders)
DELETE FROM fact_order_reviews 
WHERE order_id NOT IN (SELECT order_id FROM fact_orders);	

----------------------------------------------------------------------------------------------------------------------------

-- ============================================================================
-- STEP 6: SCHEMA ENFORCEMENT
-- Goal: Create Foreign Keys to physically enforce the Star Schema relationship.
-- ============================================================================

-- Orders -> Customers
ALTER TABLE fact_orders
ADD CONSTRAINT fk_orders_customers
FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id);

-- Items -> Orders
ALTER TABLE fact_order_items
ADD CONSTRAINT fk_items_orders
FOREIGN KEY (order_id) REFERENCES fact_orders(order_id);

-- Payments -> Orders
ALTER TABLE fact_order_payments
ADD CONSTRAINT fk_payments_orders
FOREIGN KEY (order_id) REFERENCES fact_orders(order_id);

-- Reviews -> Orders
ALTER TABLE fact_order_reviews
ADD CONSTRAINT fk_reviews_orders
FOREIGN KEY (order_id) REFERENCES fact_orders(order_id);

-- Items -> Products
ALTER TABLE fact_order_items
ADD CONSTRAINT fk_items_products
FOREIGN KEY (product_id) REFERENCES dim_products(product_id);

-- Items -> Sellers
ALTER TABLE fact_order_items
ADD CONSTRAINT fk_items_sellers
FOREIGN KEY (seller_id) REFERENCES dim_sellers(seller_id);
/*
===============================================================================
Project: Airbnb London Data Analysis 
Author: Yaroslav Pryimak
Description: 
    This script performs an End-to-End ETL process (Extract, Transform, Load).
    It imports raw CSV data (including a 1.2GB calendar file), cleans dirty data,
    handles missing values, performs type conversion, and exports optimized 
    TSV files for visualization in Tableau.

Skills & Techniques Used:
    1. DDL (Data Definition Language): Creating and modifying table structures.
    2. Data Import Optimization: Configuring MySQL server parameters (net_read_timeout, 
       max_allowed_packet) to handle large bulk loads.
    3. Advanced Data Cleaning: 
       - String Manipulation (TRIM, REPLACE, SUBSTRING).
       - Regular Expressions (REGEXP_REPLACE) for cleaning currency and percentages.
       - Handling NULLs (NULLIF, COALESCE).
    4. Type Conversion: CAST, STR_TO_DATE.
    5. Logic Implementation: CASE WHEN statements for categorizing data.
    6. Joins: LEFT JOIN for enriching the calendar table with listing prices.
    7. Data Export: Generating custom TSV files with header rows using UNION ALL 
       and handling specific character encoding/escaping issues.
===============================================================================
*/

-- Create the database schema
CREATE DATABASE airbnb_london;

USE airbnb_london;

-- ===============================================================================
-- STEP 1: CREATE RAW TABLES (STAGING LAYER)
-- Using TEXT/VARCHAR data types to ensure all data is loaded without errors,
-- even if it contains formatting issues.
-- ===============================================================================

-- 1. Calendar Table (1.2 GB)
CREATE TABLE calendar_raw (
    listing_id BIGINT,
    date TEXT,            -- Loaded as TEXT, will be converted to DATE later
    available VARCHAR(10),
    price VARCHAR(50),    -- Price contains '$' and ',' symbols, so loaded as TEXT
    adjusted_price VARCHAR(50),
    minimum_nights INT,
    maximum_nights INT
);

-- 2. Reviews Table (600 MB)
CREATE TABLE reviews_raw (
    listing_id BIGINT,
    id BIGINT,
    date TEXT,
    reviewer_id BIGINT,
    reviewer_name TEXT,
    comments TEXT
);

-- 3. Listings Table (200 MB)
-- Contains 70+ columns. TEXT is used for most fields to handle inconsistent formatting.
CREATE TABLE listings_raw (
    id TEXT,
    listing_url TEXT,
    scrape_id TEXT,
    last_scraped TEXT,
    source TEXT,
    name TEXT,
    description TEXT,
    neighborhood_overview TEXT,
    picture_url TEXT,
    host_id TEXT,
    host_url TEXT,
    host_name TEXT,
    host_since TEXT,
    host_location TEXT,
    host_about TEXT,
    host_response_time TEXT,
    host_response_rate TEXT,
    host_acceptance_rate TEXT,
    host_is_superhost TEXT,
    host_thumbnail_url TEXT,
    host_picture_url TEXT,
    host_neighbourhood TEXT,
    host_listings_count TEXT,
    host_total_listings_count TEXT,
    host_verifications TEXT,
    host_has_profile_pic TEXT,
    host_identity_verified TEXT,
    neighbourhood TEXT,
    neighbourhood_cleansed TEXT,
    neighbourhood_group_cleansed TEXT,
    latitude TEXT,
    longitude TEXT,
    property_type TEXT,
    room_type TEXT,
    accommodates TEXT,
    bathrooms TEXT,
    bathrooms_text TEXT,
    bedrooms TEXT,
    beds TEXT,
    amenities TEXT,
    price TEXT,
    minimum_nights TEXT,
    maximum_nights TEXT,
    minimum_minimum_nights TEXT,
    maximum_minimum_nights TEXT,
    minimum_maximum_nights TEXT,
    maximum_maximum_nights TEXT,
    minimum_nights_avg_ntm TEXT,
    maximum_nights_avg_ntm TEXT,
    calendar_updated TEXT,
    has_availability TEXT,
    availability_30 TEXT,
    availability_60 TEXT,
    availability_90 TEXT,
    availability_365 TEXT,
    calendar_last_scraped TEXT,
    number_of_reviews TEXT,
    number_of_reviews_ltm TEXT,
    number_of_reviews_l30d TEXT,
    availability_eoy TEXT,
    number_of_reviews_ly TEXT,
    estimated_occupancy_l365d TEXT,
    estimated_revenue_l365d TEXT,
    first_review TEXT,
    last_review TEXT,
    review_scores_rating TEXT,
    review_scores_accuracy TEXT,
    review_scores_cleanliness TEXT,
    review_scores_checkin TEXT,
    review_scores_communication TEXT,
    review_scores_location TEXT,
    review_scores_value TEXT,
    license TEXT,
    instant_bookable TEXT,
    calculated_host_listings_count TEXT,
    calculated_host_listings_count_entire_homes TEXT,
    calculated_host_listings_count_private_rooms TEXT,
    calculated_host_listings_count_shared_rooms TEXT,
    reviews_per_month TEXT
);

-- ===============================================================================
-- STEP 2: SERVER CONFIGURATION
-- Adjusting server parameters to prevent timeouts during large file imports.
-- ===============================================================================

SHOW VARIABLES LIKE "secure_file_priv";

SET GLOBAL net_read_timeout = 6000;
SET GLOBAL net_write_timeout = 6000;
SET GLOBAL connect_timeout = 6000;
SET GLOBAL max_allowed_packet = 1073741824; -- Increasing packet size to 1GB

-- ===============================================================================
-- STEP 3: LOADING DATA (ETL - EXTRACT/LOAD)
-- ===============================================================================

-- Import Calendar data 
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/calendar.csv' 
INTO TABLE calendar_raw 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS; 

-- Verification
SELECT * FROM calendar_raw;

-- Import Reviews data
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/reviews.csv' 
INTO TABLE reviews_raw 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Verification
SELECT * FROM reviews_raw;

-- Import Listings data
-- Using ESCAPED BY '"' to handle double quotes inside text fields within the CSV
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/listings.csv' 
INTO TABLE listings_raw 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
ESCAPED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Verification
SELECT * FROM listings_raw;

-- Initial data exploration check
SELECT host_is_superhost, host_has_profile_pic, host_identity_verified
FROM listings_raw
GROUP BY host_is_superhost, host_has_profile_pic, host_identity_verified;

-- ===============================================================================
-- STEP 4: DATA CLEANING & TRANSFORMATION - LISTINGS
-- ===============================================================================

DROP TABLE IF EXISTS listings_clean;

-- Cleaning Listings Table
CREATE TABLE listings_clean AS
SELECT
    CAST(id AS UNSIGNED) AS listing_id,
    name AS listing_name,
    listing_url,
    source,
    -- Cleaning Description: Treat empty strings as NULL
    NULLIF(TRIM(description), '') AS description,
    
    -- Date Formatting: Converting text dates to proper SQL DATE format
    STR_TO_DATE(NULLIF(TRIM(last_scraped), ''), '%Y-%m-%d') AS scraped_date,
    STR_TO_DATE(NULLIF(TRIM(host_since), ''), '%Y-%m-%d') AS host_since,
    
    -- Host Information Cleaning
    CAST(NULLIF(TRIM(host_id), '') AS UNSIGNED) AS host_id,
    NULLIF(TRIM(host_name), '') AS host_name,
    NULLIF(TRIM(host_location), '') AS host_location,
    NULLIF(TRIM(host_about), '') AS host_about,
    
    -- Standardizing Boolean values (t/f -> Yes/No)
    CASE 
        WHEN host_is_superhost = 't' THEN 'Yes' 
        WHEN host_is_superhost = 'f' THEN 'No' 
        ELSE NULL 
    END AS host_is_superhost,
    
    CASE 
        WHEN host_has_profile_pic = 't' THEN 'Yes' 
        WHEN host_has_profile_pic = 'f' THEN 'No' 
        ELSE NULL
    END AS host_has_profile_pic,
    
    CASE 
        WHEN host_identity_verified = 't' THEN 'Yes' 
        WHEN host_identity_verified = 'f' THEN 'No' 
        ELSE NULL
    END AS host_identity_verified,
    
    -- Cleaning Percentages: Removing '%' symbol and converting to decimal (98% -> 0.98)
    CAST(
        CASE 
            WHEN TRIM(host_response_rate) IN ('', 'N/A') THEN NULL 
            ELSE REGEXP_REPLACE(host_response_rate, '[%]', '') 
        END AS DECIMAL(5,2)
    ) / 100 AS host_response_rate,

    CAST(
        CASE 
            WHEN TRIM(host_acceptance_rate) IN ('', 'N/A') THEN NULL 
            ELSE REGEXP_REPLACE(host_acceptance_rate, '[%]', '') 
        END AS DECIMAL(5,2)
    ) / 100 AS host_acceptance_rate,
    
    -- Response Time Cleaning
    CASE 
        WHEN TRIM(host_response_time) IN ('', 'N/A') THEN NULL 
        ELSE host_response_time 
    END AS host_response_time,
    
    CAST(NULLIF(TRIM(host_listings_count), '') AS UNSIGNED) AS host_listings_count,
    CAST(NULLIF(TRIM(host_total_listings_count), '') AS UNSIGNED) AS host_total_listings_count,
    
    -- Location Data
    neighbourhood_cleansed AS neighbourhood,
    CAST(NULLIF(TRIM(latitude), '') AS DOUBLE) AS latitude,
    CAST(NULLIF(TRIM(longitude), '') AS DOUBLE) AS longitude,
    
    -- Property Details
    property_type,
    room_type,
    CAST(NULLIF(TRIM(accommodates), '') AS UNSIGNED) AS accommodates,
    NULLIF(TRIM(bathrooms_text), '') AS bathrooms_text,
    CAST(NULLIF(TRIM(bedrooms), '') AS UNSIGNED) AS bedrooms,
    CAST(NULLIF(TRIM(beds), '') AS UNSIGNED) AS beds,
    
    -- Price Cleaning: Removing '$' and ',' and converting to DECIMAL
    CAST(
        CASE 
            WHEN TRIM(price) = '' THEN NULL 
            ELSE REPLACE(REPLACE(price, '$', ''), ',', '') 
        END AS DECIMAL(10,2)
    ) AS price,
    
    -- Booking Rules
    CAST(NULLIF(TRIM(minimum_nights), '') AS UNSIGNED) AS minimum_nights,
    CAST(NULLIF(TRIM(maximum_nights), '') AS UNSIGNED) AS maximum_nights,
    
    -- Reviews & Ratings Cleaning
    CAST(NULLIF(TRIM(number_of_reviews), '') AS UNSIGNED) AS number_of_reviews,
    CAST(NULLIF(TRIM(review_scores_rating), '') AS DECIMAL(4,2)) AS review_scores_rating,
    CAST(NULLIF(TRIM(review_scores_accuracy), '') AS DECIMAL(4,2)) AS review_scores_accuracy,
    CAST(NULLIF(TRIM(review_scores_cleanliness), '') AS DECIMAL(4,2)) AS review_scores_cleanliness,
    CAST(NULLIF(TRIM(review_scores_checkin), '') AS DECIMAL(4,2)) AS review_scores_checkin,
    CAST(NULLIF(TRIM(review_scores_communication), '') AS DECIMAL(4,2)) AS review_scores_communication,
    CAST(NULLIF(TRIM(review_scores_location), '') AS DECIMAL(4,2)) AS review_scores_location,
    CAST(NULLIF(TRIM(review_scores_value), '') AS DECIMAL(4,2)) AS review_scores_value,
    
    CAST(NULLIF(TRIM(reviews_per_month), '') AS DECIMAL(5,2)) AS reviews_per_month

FROM listings_raw
-- Filtering out rows where ID is not a number (headers/garbage)
WHERE id REGEXP '^[0-9]+$';

-- Verification checks
SELECT * FROM listings_clean;

SELECT host_is_superhost, COUNT(*) 
FROM listings_clean 
GROUP BY host_is_superhost;

-- ===============================================================================
-- STEP 5: DATA CLEANING & TRANSFORMATION - CALENDAR
-- ===============================================================================

SELECT * FROM calendar_raw;

-- Checking data quality for 'available' columns
SELECT available, price, adjusted_price, count(*)
FROM calendar_raw
WHERE available = 't' 
GROUP BY available, price, adjusted_price
LIMIT 100;

DROP TABLE IF EXISTS calendar_clean;

-- Initial creation of calendar_clean (Converting types and cleaning prices)
CREATE TABLE calendar_clean AS
SELECT
    CAST(listing_id AS UNSIGNED) AS listing_id,
    STR_TO_DATE(NULLIF(TRIM(date), ''), '%Y-%m-%d') AS date,    
    
    -- Standardizing availability status
    CASE 
        WHEN available = 't' THEN 'Available'
        ELSE 'Booked'
    END AS available,
    
    -- Price Cleaning
    CAST(
        CASE 
            WHEN TRIM(price) IS NULL OR TRIM(price) = '' THEN NULL 
            ELSE REPLACE(REPLACE(price, '$', ''), ',', '') 
        END AS DECIMAL(10,2)
    ) AS price,
    
    -- Adjusted Price Cleaning
    CAST(
        CASE 
            WHEN TRIM(adjusted_price) IS NULL OR TRIM(adjusted_price) = '' THEN NULL 
            ELSE REPLACE(REPLACE(adjusted_price, '$', ''), ',', '') 
        END AS DECIMAL(10,2)
    ) AS adjusted_price,
    
    CAST(minimum_nights AS UNSIGNED) AS minimum_nights,
    CAST(maximum_nights AS UNSIGNED) AS maximum_nights

FROM calendar_raw
WHERE listing_id REGEXP '^[0-9]+$';

-- Check for missing prices
SELECT * FROM calendar_clean 
WHERE price IS NOT NULL 
LIMIT 50;

-- OPTION 1: Update prices using JOIN (Slower on large datasets)
-- UPDATE calendar_clean c
-- JOIN listings_clean l ON c.listing_id = l.listing_id
-- SET c.price = l.price
-- WHERE c.price IS NULL AND c.available = 'Available';

-- OPTION 2 (OPTIMIZED): Re-create table using JOIN to populate missing prices
-- This approach is faster for 35M+ rows as it avoids transaction logging overhead.
DROP TABLE IF EXISTS calendar_clean;

CREATE TABLE calendar_clean AS
SELECT 
    CAST(c.listing_id AS UNSIGNED) AS listing_id,
    STR_TO_DATE(NULLIF(TRIM(c.date), ''), '%Y-%m-%d') AS date,
    
    CASE 
        WHEN c.available = 't' THEN 'Available'
        ELSE 'Booked'
    END AS available,
    
    -- Backfill Logic:
    -- If calendar price exists -> use it.
    -- If NULL -> use the base price from the listings table.
    CAST(
        COALESCE(
            NULLIF(REPLACE(REPLACE(c.price, '$', ''), ',', ''), ''), -- Calendar Price
            l.price -- Listing Price (Fallback)
        ) AS DECIMAL(10,2)
    ) AS price,
    
    CAST(
        CASE 
            WHEN TRIM(c.adjusted_price) IS NULL OR TRIM(c.adjusted_price) = '' THEN NULL 
            ELSE REPLACE(REPLACE(c.adjusted_price, '$', ''), ',', '') 
        END AS DECIMAL(10,2)
    ) AS adjusted_price,
    
    CAST(c.minimum_nights AS UNSIGNED) AS minimum_nights,
    CAST(c.maximum_nights AS UNSIGNED) AS maximum_nights

FROM calendar_raw c
-- Left join ensures we keep calendar dates even if listing details are missing
LEFT JOIN listings_clean l ON c.listing_id = l.listing_id 
WHERE c.listing_id REGEXP '^[0-9]+$';

-- Verification checks
SELECT * FROM calendar_clean;

-- Remove unused column (adjusted_price was mostly NULL)
ALTER TABLE calendar_clean
DROP COLUMN adjusted_price;

-- ===============================================================================
-- STEP 6: DATA CLEANING & TRANSFORMATION - REVIEWS
-- ===============================================================================

SELECT * FROM reviews_raw;

DROP TABLE IF EXISTS reviews_clean;

CREATE TABLE reviews_clean AS
SELECT 
    CAST(listing_id AS UNSIGNED) AS listing_id,
    CAST(id AS UNSIGNED) AS review_id,
    
    -- Date Transformation
    STR_TO_DATE(NULLIF(TRIM(date), ''), '%Y-%m-%d') AS review_date,
    
    CAST(reviewer_id AS UNSIGNED) AS reviewer_id,
    NULLIF(TRIM(reviewer_name), '') AS reviewer_name,
    
    -- Truncate comments to 1000 chars to optimize storage and visualization performance
    LEFT(NULLIF(TRIM(comments), ''), 1000) AS comments

FROM reviews_raw
WHERE listing_id REGEXP '^[0-9]+$'; 

SELECT * FROM reviews_clean;

-- ===============================================================================
-- STEP 7: DATA EXPORT FOR TABLEAU
-- Strategy: Export as TSV (Tab Separated Values) to handle internal commas in text.
-- Also thoroughly cleaning text fields (removing Tabs, Enters, Quotes) to prevent
-- the CSV structure from breaking in Tableau.
-- ===============================================================================

-- 1. Export Listings
(SELECT 'listing_id', 'listing_name', 'listing_url', 'source', 'description', 'scraped_date', 'host_since', 
        'host_id', 'host_name', 'host_location', 'host_about', 'host_is_superhost', 'host_has_profile_pic', 
        'host_identity_verified', 'host_response_rate', 'host_acceptance_rate', 'host_response_time', 
        'host_listings_count', 'host_total_listings_count', 'neighbourhood', 'latitude', 'longitude', 
        'property_type', 'room_type', 'accommodates', 'bathrooms_text', 'bedrooms', 'beds', 'price', 
        'minimum_nights', 'maximum_nights', 'number_of_reviews', 'review_scores_rating', 'review_scores_accuracy', 
        'review_scores_cleanliness', 'review_scores_checkin', 'review_scores_communication', 'review_scores_location', 
        'review_scores_value', 'reviews_per_month')
UNION ALL
(SELECT 
    listing_id, 
    -- Cleaning Name: Removing tabs, newlines, and converting double quotes to single quotes
    REPLACE(REPLACE(REPLACE(REPLACE(IFNULL(listing_name, ''), '\t', ' '), '\r', ' '), '\n', ' '), '"', '\'') AS listing_name,
    listing_url, 
    source, 
    -- Cleaning Description
    REPLACE(REPLACE(REPLACE(REPLACE(IFNULL(description, ''), '\t', ' '), '\r', ' '), '\n', ' '), '"', '\'') AS description,
    scraped_date, 
    host_since, 
    host_id, 
    host_name, 
    -- Cleaning Location and Host About
    REPLACE(REPLACE(REPLACE(REPLACE(IFNULL(host_location, ''), '\t', ' '), '\r', ' '), '\n', ' '), '"', '\'') AS host_location, 
    REPLACE(REPLACE(REPLACE(REPLACE(IFNULL(host_about, ''), '\t', ' '), '\r', ' '), '\n', ' '), '"', '\'') AS host_about,
    host_is_superhost, 
    host_has_profile_pic, 
    host_identity_verified, 
    host_response_rate, 
    host_acceptance_rate, 
    host_response_time, 
    host_listings_count, 
    host_total_listings_count, 
    REPLACE(REPLACE(REPLACE(REPLACE(IFNULL(neighbourhood, ''), '\t', ' '), '\r', ' '), '\n', ' '), '"', '\'') AS neighbourhood, 
    latitude, 
    longitude, 
    property_type, 
    room_type, 
    accommodates, 
    bathrooms_text, 
    bedrooms, 
    beds, 
    price, 
    minimum_nights, 
    maximum_nights, 
    number_of_reviews, 
    review_scores_rating, 
    review_scores_accuracy, 
    review_scores_cleanliness, 
    review_scores_checkin, 
    review_scores_communication, 
    review_scores_location, 
    review_scores_value, 
    reviews_per_month
FROM listings_clean)
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/listings_final_tsv.txt'
CHARACTER SET utf8mb4 
FIELDS TERMINATED BY '\t'  -- Using TAB as delimiter
ENCLOSED BY ''             -- Disabling quotes enclosure to prevent parsing errors
ESCAPED BY ''
LINES TERMINATED BY '\r\n';

-- 2. Export Reviews
(SELECT 'listing_id', 'review_id', 'review_date', 'reviewer_id', 'reviewer_name', 'comments')
UNION ALL
(SELECT 
    listing_id, 
    review_id, 
    review_date, 
    reviewer_id, 
    -- Cleaning Reviewer Name
    REPLACE(REPLACE(REPLACE(REPLACE(IFNULL(reviewer_name, ''), '\t', ' '), '\r', ' '), '\n', ' '), '"', '\'') AS reviewer_name,
    -- Cleaning Comments (Crucial step for text analysis)
    REPLACE(REPLACE(REPLACE(REPLACE(IFNULL(comments, ''), '\t', ' '), '\r', ' '), '\n', ' '), '"', '\'') AS comments
FROM reviews_clean)
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/reviews_final_tsv.txt'
CHARACTER SET utf8mb4 
FIELDS TERMINATED BY '\t' 
ENCLOSED BY '' 
ESCAPED BY ''
LINES TERMINATED BY '\r\n';

-- 3. Export Calendar
(SELECT 'listing_id', 'date', 'available', 'price', 'minimum_nights', 'maximum_nights')
UNION ALL
(SELECT 
    listing_id, 
    date, 
    available, 
    price, 
    minimum_nights, 
    maximum_nights
FROM calendar_clean)
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.4/Uploads/calendar_final_tsv.txt'
CHARACTER SET utf8mb4 
FIELDS TERMINATED BY '\t' 
ENCLOSED BY '' 
ESCAPED BY ''
LINES TERMINATED BY '\r\n';
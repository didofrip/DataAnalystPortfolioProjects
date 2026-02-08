/*
===============================================================================
Project: Airbnb London Data Analysis (SQL Analysis)
Author: Yaroslav Pryimak
Description: 
    This script performs advanced analysis on the cleaned Airbnb data.
    It covers pricing trends, seasonality, property ranking, text mining, 
    and performance metrics for Superhosts.
    
Skills & Techniques Used:
    1. Advanced Aggregation: Using GROUP BY and HAVING to filter aggregated results.
    2. Window Functions: Using ROW_NUMBER() for ranking and AVG() OVER() for comparing 
       individual rows against group statistics.
    3. Common Table Expressions (CTEs): Using WITH clauses to improve query readability 
       and modularity.
    4. Regular Expressions: Using REGEXP for pattern matching in text analysis.
    5. Date Manipulation: Extracting and formatting specific date parts using DATE_FORMAT.
    6. Database Objects: Creating VIEWS for reusable logic and TEMPORARY TABLES for 
       session-based data storage.
    7. Joins: Combining data from multiple tables using INNER JOIN.
===============================================================================
*/

USE airbnb_london;

-- ===============================================================================
-- TASK 1: Analysis of pricing policy by region
-- Objective: Identify "premium" vs "budget" neighbourhoods.
-- ===============================================================================
SELECT 
	neighbourhood,
    COUNT(listing_id) AS listing_count,
    AVG(price) AS avg_price,
    MIN(price) AS min_price,
    MAX(price) AS max_price
FROM listings_clean
GROUP BY neighbourhood
-- Filtering out areas with low sample size to ensure statistical significance
-- (The current minimum number of listings is 477, but this protects against future data changes)
HAVING listing_count > 50
ORDER BY avg_price DESC;

-- ===============================================================================
-- TASK 2: Seasonality and potential income
-- Objective: Determine the most profitable months based on booked revenue.
-- ===============================================================================
SELECT 
	available,
    SUM(price) AS Revenue,
    DATE_FORMAT(date, '%M, %Y') AS month_of_year
FROM calendar_clean
WHERE available = 'Booked'
GROUP BY month_of_year
ORDER BY Revenue DESC;

-- ===============================================================================
-- TASK 3: Top-3 most expensive listings in each category
-- Objective: Select flagship properties using Window Functions.
-- ===============================================================================
WITH price_rank_by_property_type AS (
	SELECT 
		listing_name,
		neighbourhood,
		property_type,
		price,
        -- Assign a rank to each property within its type, ordered by price (High to Low)
		ROW_NUMBER() OVER (PARTITION BY property_type ORDER BY price DESC) AS price_rank,
		listing_url
	FROM listings_clean)
SELECT 
	listing_name,
    neighbourhood,
	property_type,
    price,
    price_rank,
    listing_url
FROM price_rank_by_property_type
WHERE price_rank <= 3 
ORDER BY property_type, price DESC;

/* Note: 
   As observed, some prices (e.g., >1M) appear to be outliers or fake listings.
   In a real-world scenario, I would add a 'WHERE price < X' filter 
   in the CTE to exclude unrealistic values before ranking.
*/

-- ===============================================================================
-- TASK 4: Comparison of property price vs. neighbourhood average
-- Objective: Identify properties priced above their local market average.
-- ===============================================================================
WITH avg_neighbourhood_price AS (
	SELECT 
		listing_name,
		price,
        -- Calculate the average price for the specific neighbourhood without collapsing rows
		AVG(price) OVER (PARTITION BY neighbourhood) AS avg_price,
        -- Calculate the difference between the specific listing price and the area average
        price - AVG(price) OVER (PARTITION BY neighbourhood) AS price_diff,
		neighbourhood,
		review_scores_rating,
        listing_url
	FROM listings_clean)
SELECT 
	listing_name,
	price,
    -- Formatting for readability
	TRUNCATE(avg_price, 2) AS avg_price,
    TRUNCATE(price_diff, 2) AS price_diff,
    neighbourhood,
    review_scores_rating,
    listing_url
FROM avg_neighbourhood_price
-- Filtering for properties that are more expensive than average
WHERE price_diff > 0
ORDER BY price_diff DESC;

-- ===============================================================================
-- TASK 5: Searching for problematic apartments through reviews
-- Objective: Text mining using REGEXP to find negative feedback (dirt/noise).
-- ===============================================================================
SELECT 
	listings_clean.listing_id,
    listings_clean.host_id,
    listings_clean.host_name,
    listings_clean.listing_name,
	reviews_clean.review_id,
	reviews_clean.comments
FROM reviews_clean
JOIN listings_clean
	ON listings_clean.listing_id = reviews_clean.listing_id
-- Using Regular Expressions to search for any of the keywords
WHERE LOWER(comments) REGEXP 'dirty|filthy|noise|loud';

/* Note: 
   This approach uses keyword matching. It may generate false positives 
   (e.g., "There was no noise").
*/

-- ===============================================================================
-- TASK 6: Analysis of "Superhosts"
-- Objective: Create a View to analyze underperforming Superhosts.
-- ===============================================================================
CREATE OR REPLACE VIEW view_superhost_stats AS 
SELECT 
	host_id,
    host_name,
	COUNT(listing_id) AS num_of_listings,
    AVG(review_scores_rating) AS avg_rating,
    SUM(number_of_reviews) AS sum_of_reviews
FROM listings_clean
WHERE host_is_superhost = 'Yes'
GROUP BY host_id, host_name;

SELECT * 
FROM view_superhost_stats
WHERE avg_rating < 4.5
ORDER BY sum_of_reviews DESC, avg_rating DESC;

-- ===============================================================================
-- TASK 7: Preparing a report for distribution
-- Objective: Use a Temporary Table to optimize repeated queries for a specific cohort.
-- ===============================================================================
CREATE TEMPORARY TABLE IF NOT EXISTS temp_recent_reviewers AS 
SELECT DISTINCT
	reviewer_id,
    reviewer_name
FROM reviews_clean
-- Filtering for reviews left in 2024 using pattern matching
WHERE review_date LIKE '2024-%';

-- Verify the data in the temporary table
SELECT * 
FROM temp_recent_reviewers


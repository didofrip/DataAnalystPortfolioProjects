/*
===============================================================================
Project: SQL Data Cleaning & Standardization
Author: Yaroslav Pryimak
Description: 
    This script initiates the data cleaning pipeline (ETL process). 
    It focuses on the staging phase: creating a flexible table structure to 
    import raw CSV data without ingestion errors. The script utilizes broad 
    data types to handle "dirty" source data before further cleaning and 
    standardization steps are applied.

Skills & Techniques Used:
    1. DDL (Data Definition Language): Using CREATE TABLE to design the staging schema.
    2. Data Import Strategy: Implementing a "broad schema" approach (VARCHAR(300)) 
       to prevent truncation errors and type mismatches during initial bulk load.
    3. DML (Data Manipulation): Using INSERT statements to populate the staging table.
    4. Data Exploration & Validation: 
       - SELECT & DISTINCT to identify unique values and anomalies.
       - ORDER BY to organize data for visual inspection.
       - HAVING to filter aggregated data (e.g., checking for duplicates).
===============================================================================
*/

CREATE TABLE clients (
	id VARCHAR(300),
    first_name VARCHAR(300),
    last_name VARCHAR(300),
    gender VARCHAR(300),
	email VARCHAR(300),
    salary VARCHAR(300),
    full_address VARCHAR(300),
    join_date VARCHAR(300)
);

-- Checking if all data has been imported 
SELECT *
FROM clients;

-- Checking for duplicates 
SELECT id, COUNT(*) 
FROM clients
GROUP BY id
HAVING COUNT(*) > 1;

---------------------------------------------------------------------------------


-- Create a table ‘clients_cleaned’ with clean data. 
-- Keep the ‘clients’ table in its original form in case of accidental deletion of any data 

CREATE TABLE clients_cleaned (
	id INT PRIMARY KEY,
    first_name VARCHAR(300),
    last_name VARCHAR(300),
    gender VARCHAR(50),
	email VARCHAR(300) DEFAULT NULL,
    salary DECIMAL(10,2),
    city VARCHAR(300),
    state VARCHAR(300),
    join_date DATE
);

-- Transfer data from the original table and clean it up right away 
INSERT INTO clients_cleaned (id, first_name, last_name, gender, email, salary, city, state, join_date)
-- Using ‘DISTINCT’ to remove duplicates 
SELECT DISTINCT 
	-- Changing the data type from ‘VARCHAR’ to 'SIGNED'  
	CAST(id AS SIGNED) AS id, 
    
    -- Bringing the first name to a standard form
	CONCAT(UPPER(LEFT(LOWER(first_name), 1)), LOWER(SUBSTRING(first_name, 2))) AS first_name, 
    
    -- Recording ‘Unknown’ in cases where the last name is missing 
    CASE
		WHEN last_name = '' OR last_name IS NULL THEN 'Unknown'
        ELSE last_name
	END AS last_name, 
    
    -- Bringing everything to a single standard: 'Male' and 'Female' 
	CASE
		WHEN gender IN ('male', 'M') THEN 'Male'
        WHEN gender IN ('fem', 'F', 'female') THEN 'Female'
        ELSE 'Unknown'
	END AS gender, 
    
    -- Replace ‘NULL’ with ‘No Email’ if there is no email address
    COALESCE(NULLIF(email, ''), 'No Email') AS email,
    
    -- Removing the ‘$’ sign and changing the data format to 'DECIMAL(10,2)'
    CAST((REPLACE(salary, '$', '')) AS DECIMAL(10,2)) AS salary, 
    
    -- Split ‘full_address’ into two separate columns: ‘city’ and 'state'
    TRIM(SUBSTRING_INDEX(full_address, ',', 1)) AS city,
    
	TRIM(SUBSTR(full_address, -2)) AS state, 
    
    -- Changing the data format from ‘VARCHAR’ as ‘dd.mm.yyyy’ to ‘DATE’ in the standard format 'yyyy-mm-dd'
    STR_TO_DATE(join_date, '%d.%m.%Y') AS join_date
FROM clients;

-- Getting a clean table 
SELECT * 
FROM clients_cleaned; 

---------------------------------------------------------------------------------

-- The final table for communication with customers.
-- P.S.: Data with missing email addresses is excluded
SELECT * 
FROM clients_cleaned
WHERE email LIKE '%@%'
ORDER BY join_date DESC;

/*
============================================================
 Data Quality Check Script - bronze.sales_customers
============================================================
 Table       : bronze.sales_customers
 Purpose     : Perform data quality checks on the customers table 
               to identify dirty, inconsistent, or invalid data 
               before transforming into the Silver layer.
 

 Notes:
   - Apostrophes and hyphens in names are considered valid.
   - Results should guide cleaning rules for the Silver layer.
============================================================
*/
----------------------
--customer_id column
----------------------

-- 1. Check duplicate customer_id
SELECT
    customer_id, 
    COUNT(*) AS cnt
FROM bronze.sales_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- 2. Check for null or placeholder values in customer_id
SELECT 
    COUNT(*) AS null_count
FROM bronze.sales_customers
WHERE customer_id = 'NULL';


----------------------
--first_name column
----------------------

-- 1. Check for null/blank values in first_name
SELECT 
    COUNT(*) AS null_first_name
FROM bronze.sales_customers
WHERE first_name = 'NULL' OR TRIM(first_name) = '';

-- 2. Check frequency of duplicate first names
SELECT 
    first_name,
    COUNT(*) AS Count_names
FROM bronze.sales_customers
GROUP BY first_name
HAVING COUNT(*) > 1;

-- 3. Check invalid characters or digits in first_name
SELECT DISTINCT
    first_name
FROM bronze.sales_customers
WHERE first_name LIKE '%[0-9]%' OR first_name LIKE '%[^a-zA-Z ]%';

-- 4. Check unusually long first names
SELECT TOP 10
    first_name, LEN(first_name) AS length
FROM bronze.sales_customers
ORDER BY LEN(first_name) DESC;


----------------------
--last_name column
----------------------

-- 1. Check for null/blank values in last_name
SELECT 
    COUNT(*) AS null_last_name
FROM bronze.sales_customers
WHERE last_name = 'NULL' OR TRIM(last_name) = '';

-- 2. Check frequency of duplicate last names
SELECT 
    last_name,
    COUNT(*) AS Count_names
FROM bronze.sales_customers
GROUP BY last_name
HAVING COUNT(*) > 1;

-- 3. Check invalid characters or digits in last_name
SELECT DISTINCT 
    last_name
FROM bronze.sales_customers
WHERE last_name LIKE '%[0-9]%' OR last_name LIKE '%[^a-zA-Z ]%';

-- 4. Check unusually long last names
SELECT TOP 10 
    last_name, LEN(last_name) AS length
FROM bronze.sales_customers
ORDER BY LEN(last_name) DESC;


----------------------
--first_name + last_name combo
----------------------

-- 1. Check duplicates on full name (first_name + last_name)
SELECT 
    first_name,
    last_name,
    COUNT(*) AS Count_names
FROM bronze.sales_customers
GROUP BY first_name, last_name
HAVING COUNT(*) > 1;


----------------------
--phone column
----------------------

-- 1. Check how many rows have 'NULL' string 
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.sales_customers
WHERE phone = 'NULL';


-- 2. Check actual SQL NULLs 
SELECT 
    COUNT(*) AS null_sql_count
FROM bronze.sales_customers
WHERE phone IS NULL;

-- 3. Check duplicate phone numbers (excluding 'NULL')
SELECT 
    phone,
    COUNT(*) AS cnt
FROM bronze.sales_customers
WHERE phone <> 'NULL'
GROUP BY phone
HAVING COUNT(*) > 1;

-- 4. Check phone numbers with invalid characters
SELECT DISTINCT 
    phone
FROM bronze.sales_customers
WHERE phone LIKE '%[0-9()% +-]%';

--Removing space, brackets and hyphen
--replacing NULLs with actual SQL NULLs

SELECT 
    phone,
    CASE 
        WHEN phone = 'NULL' THEN NULL
        ELSE
            REPLACE(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), '-', ''), ' ', '') 
    END AS cleaned_phone 
FROM bronze.sales_customers;


-- 5. Check phone numbers with unusual length
SELECT 
    phone, 
    LEN(phone) AS length
FROM bronze.sales_customers
WHERE phone <> 'NULL'
ORDER BY LEN(phone) DESC;

-- 6. Profile: distribution of phone number lengths
SELECT 
    LEN(phone) AS length,
    COUNT(*) AS cnt
FROM bronze.sales_customers
WHERE phone <> 'NULL'
GROUP BY LEN(phone)
ORDER BY cnt DESC;


----------------------
--email column
----------------------

-- 1. Count NULL / blank emails
SELECT 
    COUNT(*) AS null_count
FROM bronze.sales_customers
WHERE email = 'NULL' OR TRIM(email) = '';


-- 2. Replace 'NULL' strings with actual NULL
SELECT 
    email,
    CASE 
        WHEN email = 'NULL' OR TRIM(email) = '' THEN NULL 
        ELSE email 
    END AS email_cleaned
FROM bronze.sales_customers;


-- 3. Check duplicate emails
SELECT 
    email, 
    COUNT(*) AS cnt
FROM bronze.sales_customers
WHERE email != 'NULL' AND TRIM(email) != ''
GROUP BY email
HAVING COUNT(*) > 1;

-- 4. Emails without '@' or '.'
SELECT DISTINCT 
    email
FROM bronze.sales_customers
WHERE email NOT LIKE '%@%' OR email NOT LIKE '%.%';

-- 5. Emails with spaces (invalid)
SELECT DISTINCT 
    email
FROM bronze.sales_customers
WHERE email LIKE '% %';

-- 6. Emails with multiple '@'
SELECT DISTINCT
    email
FROM bronze.sales_customers
WHERE LEN(email) - LEN(REPLACE(email, '@', '')) > 1;

-- 7. Top 10 longest emails
SELECT TOP 10
    email,
    LEN(email) AS length
FROM bronze.sales_customers
ORDER BY LEN(email) DESC;

-- 8. Distribution of email lengths
SELECT 
    LEN(email) AS length, 
    COUNT(*) AS cnt
FROM bronze.sales_customers
WHERE email <> 'NULL' AND TRIM(email) <> ''
GROUP BY LEN(email)
ORDER BY cnt DESC;
 


-----------------
--street column
-----------------

-- 1. Check for null or placeholder values
SELECT 
    COUNT(*) AS null_street_count
FROM bronze.sales_customers
WHERE street = 'NULL' OR TRIM(street) = '' OR street IS NULL;

-- 2. Find duplicate street values (to see if many customers share same street)
SELECT 
    street,
    COUNT(*) AS cnt
FROM bronze.sales_customers
GROUP BY street
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

-- 3. Check for invalid characters (anything other than letters, digits, space, period, hyphen, etc)
SELECT DISTINCT 
    street
FROM bronze.sales_customers
WHERE street LIKE '%[^a-zA-Z0-9 .-]%';

-- 4. Check unusually long street values
SELECT TOP 10 
    street, LEN(street) AS length
FROM bronze.sales_customers
ORDER BY LEN(street) DESC;

-- 5. Check unusually short street values
SELECT TOP 10 
    street, LEN(street) AS length
FROM bronze.sales_customers
ORDER BY LEN(street) ASC;


---------------
--city column
---------------

-- 1. Check for null or placeholder values
SELECT 
    COUNT(*) AS null_city_count
FROM bronze.sales_customers
WHERE city = 'NULL' OR TRIM(city) = '' OR city IS NULL;

-- 2. Frequency of duplicate city names 
SELECT 
    city,
    COUNT(*) AS cnt
FROM bronze.sales_customers
GROUP BY city
ORDER BY cnt DESC;

-- 3. Check for invalid characters (should only be alphabets, spaces, and hyphens)
SELECT DISTINCT 
    city
FROM bronze.sales_customers
WHERE city LIKE '%[^a-zA-Z ]%';

-- 4. Check unusually long city names
SELECT TOP 10 
    city, LEN(city) AS length
FROM bronze.sales_customers
ORDER BY LEN(city) DESC;

-- 5. Check unusually short city names
SELECT TOP 10 
    city, LEN(city) AS length
FROM bronze.sales_customers
ORDER BY LEN(city) ASC;



---------------
--state column
---------------

-- 1. Check for null or placeholder values
SELECT 
    COUNT(*) AS null_state_count
FROM bronze.sales_customers
WHERE state = 'NULL' OR TRIM(state) = '' OR state IS NULL;

-- 2. Frequency of state values
SELECT 
    state,
    COUNT(*) AS cnt
FROM bronze.sales_customers
GROUP BY state
ORDER BY cnt DESC;

-- 3. Check for invalid characters
SELECT DISTINCT 
    state
FROM bronze.sales_customers
WHERE state LIKE '%[^a-zA-Z ]%';

-- 4. Check length of state values 
-- (abbreviations should be 2 chars; full names usually longer)
SELECT DISTINCT 
    state, LEN(state) AS length
FROM bronze.sales_customers
ORDER BY LEN(state) DESC;

-- 5. Identify potential mismatches (mixed of full names and abbreviations)
SELECT 
    state,
    COUNT(*) AS cnt
FROM bronze.sales_customers
GROUP BY state
HAVING COUNT(DISTINCT LEN(state)) > 1;


---------------
--zip_code column
---------------

-- 1. Check for null or placeholder values
SELECT 
    COUNT(*) AS null_zip_count
FROM bronze.sales_customers
WHERE zip_code = 'NULL' OR TRIM(zip_code) = '' OR zip_code IS NULL;

-- 2. Frequency of zip codes
SELECT 
    zip_code,
    COUNT(*) AS cnt
FROM bronze.sales_customers
GROUP BY zip_code
ORDER BY cnt DESC;

-- 3. Check for non-numeric or invalid characters
SELECT DISTINCT 
    zip_code
FROM bronze.sales_customers
WHERE zip_code LIKE '%[^0-9]%';

-- 4. Check lengths of zip codes 
-- (standard US zip: 5 digits, extended zip: 9 digits with dash)
SELECT DISTINCT 
    zip_code, LEN(zip_code) AS length
FROM bronze.sales_customers
ORDER BY LEN(zip_code);


--================
--SELECT staement
--================

-----------------------------------------------------
-- Cleaned data selection for silver.sales_customers
-----------------------------------------------------
SELECT
    -- Replace 'NULL' string with actual NULL
    CAST(NULLIF(customer_id, 'NULL') AS INT) AS customer_id,

    NULLIF(TRIM(first_name), 'NULL') AS first_name,
    NULLIF(TRIM(last_name), 'NULL') AS last_name,

    -- Phone: remove brackets, spaces, hyphens and handle 'NULL'
    CAST
    (
        CASE 
            WHEN phone = 'NULL' THEN NULL
            ELSE REPLACE(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), '-', ''), ' ', '')
        END 
        AS VARCHAR(10)
    )   AS phone,

    NULLIF(email, 'NULL') AS email,
    NULLIF(street, 'NULL') AS street,
    NULLIF(city, 'NULL') AS city,
    CAST(NULLIF(state, 'NULL') AS CHAR(2)) AS state,

    CAST(NULLIF(zip_code, 'NULL') AS CHAR(5)) AS zip_code,
    -- Derived: full address
    CAST(TRIM(NULLIF(street, 'NULL')) + ', ' + TRIM(NULLIF(city, 'NULL')) + ', ' + CAST(NULLIF(state, 'NULL') AS CHAR(2)) + ', ' + CAST(NULLIF(zip_code, 'NULL') AS CHAR(5)) AS VARCHAR(370)) AS full_address


FROM bronze.sales_customers;
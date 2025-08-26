/*
============================================================
 Data Quality Check Script - bronze.production_products
============================================================
 Table       : bronze.production_products
 Purpose     : Perform data quality checks on the production_products table
               to identify dirty, inconsistent, or invalid data 
               before transforming into the Silver layer.

 Notes:
   - Product names may contain spaces, hyphens, apostrophes.
   - Results should guide cleaning rules for the Silver layer.
============================================================
*/

----------------------
-- product_id column
----------------------

-- 1. Check for 'NULL' strings or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.production_products
WHERE product_id = 'NULL' OR TRIM(product_id) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.production_products
WHERE product_id IS NULL;

-- 3. Check for non-numeric values
SELECT DISTINCT
    product_id
FROM bronze.production_products
WHERE TRY_CAST(product_id AS INT) IS NULL
  AND product_id <> 'NULL';

-- 4. Check total distinct product_ids
SELECT 
    COUNT(DISTINCT product_id) AS distinct_products,
    COUNT(*) AS total_rows
FROM bronze.production_products;


----------------------
-- product_name column
----------------------

-- 1. Check for 'NULL' strings or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.production_products
WHERE product_name = 'NULL' OR TRIM(product_name) = '';

-- 2. Check for unusually long product names
SELECT TOP 10
    product_name, LEN(product_name) AS length
FROM bronze.production_products
ORDER BY LEN(product_name) DESC;

-- 3. Check for invalid characters (allow letters, digits, spaces, hyphens, apostrophes)
SELECT DISTINCT
    product_name
FROM bronze.production_products
WHERE product_name LIKE '%[^a-zA-Z0-9 ]%';


SELECT 
    *
FROM
(
    SELECT 
        product_id,
        product_name AS original_name,
        REPLACE(NULLIF(TRIM(product_name), 'NULL'), '"', '') AS cleaned_name,
        CASE 
            WHEN product_name <> REPLACE(NULLIF(TRIM(product_name), 'NULL'), '"', '') 
            THEN 'Changed'
            ELSE 'Same'
        END AS flag
    FROM bronze.production_products
) AS T
WHERE flag = 'Changed'


----------------------
-- brand_id column
----------------------

-- 1. Check for 'NULL' strings or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.production_products
WHERE brand_id = 'NULL' OR TRIM(brand_id) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.production_products
WHERE brand_id IS NULL;

-- 3. Check for non-numeric values
SELECT DISTINCT
    brand_id
FROM bronze.production_products
WHERE TRY_CAST(brand_id AS INT) IS NULL
  AND brand_id <> 'NULL';


----------------------
-- category_id column
----------------------

-- 1. Check for 'NULL' strings or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.production_products
WHERE category_id = 'NULL' OR TRIM(category_id) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.production_products
WHERE category_id IS NULL;

-- 3. Check for non-numeric values
SELECT DISTINCT
    category_id
FROM bronze.production_products
WHERE TRY_CAST(category_id AS INT) IS NULL
  AND category_id <> 'NULL';


----------------------
-- model_year column
----------------------

-- 1. Check for 'NULL' strings or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.production_products
WHERE model_year = 'NULL' OR TRIM(model_year) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.production_products
WHERE model_year IS NULL;

-- 3. Check for non-numeric values
SELECT DISTINCT
    model_year
FROM bronze.production_products
WHERE TRY_CAST(model_year AS INT) IS NULL
  AND model_year <> 'NULL';

-- 4. Check range of model_year
SELECT 
    MIN(TRY_CAST(model_year AS INT)) AS min_year,
    MAX(TRY_CAST(model_year AS INT)) AS max_year
FROM bronze.production_products;


----------------------
-- list_price column
----------------------

-- 1. Check for 'NULL' strings or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.production_products
WHERE list_price = 'NULL' OR TRIM(list_price) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.production_products
WHERE list_price IS NULL;

-- 3. Check for non-numeric values
SELECT DISTINCT
    list_price
FROM bronze.production_products
WHERE TRY_CAST(list_price AS DECIMAL(10,2)) IS NULL
  AND list_price <> 'NULL';

-- 4. Check range of list_price
SELECT 
    MIN(TRY_CAST(list_price AS DECIMAL(10,2))) AS min_price,
    MAX(TRY_CAST(list_price AS DECIMAL(10,2))) AS max_price
FROM bronze.production_products;

-- 5. Check for suspicious prices (<=0)
SELECT 
    list_price, COUNT(*) AS cnt
FROM bronze.production_products
WHERE TRY_CAST(list_price AS DECIMAL(10,2)) <= 0
GROUP BY list_price
ORDER BY cnt DESC;



--==================
--SELECT statement
--==================


SELECT
    CAST(NULLIF(product_id, 'NULL') AS INT) AS product_id,
    
    -- Remove double quotes and trim
    REPLACE(NULLIF(TRIM(product_name), 'NULL'), '"', '') AS product_name,
    
    CAST(NULLIF(brand_id, 'NULL') AS INT) AS brand_id,
    CAST(NULLIF(category_id, 'NULL') AS INT) AS category_id,
    CAST(NULLIF(model_year, 'NULL') AS INT) AS model_year,
    CAST(NULLIF(list_price, 'NULL') AS DECIMAL(10,2)) AS list_price

FROM bronze.production_products;

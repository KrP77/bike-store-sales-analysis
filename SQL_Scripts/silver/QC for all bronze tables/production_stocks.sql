/*
============================================================
 Data Quality Check Script - bronze.production_stocks
============================================================
 Table       : bronze.production_stocks
 Purpose     : Perform data quality checks on the production_stocks table
               to identify dirty, inconsistent, or invalid data 
               before transforming into the Silver layer.

 Notes:
   - Product names may contain spaces, hyphens, apostrophes.
   - Results should guide cleaning rules for the Silver layer.
============================================================
*/

----------------------
-- store_id column
----------------------

-- 1. Check for 'NULL' strings or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.production_stocks
WHERE store_id = 'NULL' OR TRIM(store_id) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.production_stocks
WHERE store_id IS NULL;

-- 3. Check non-numeric values
SELECT DISTINCT
    store_id
FROM bronze.production_stocks
WHERE TRY_CAST(store_id AS INT) IS NULL AND store_id <> 'NULL';


----------------------
-- product_id column
----------------------

-- 1. Check for 'NULL' strings or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.production_stocks
WHERE product_id = 'NULL' OR TRIM(product_id) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.production_stocks
WHERE product_id IS NULL;

-- 3. Check non-numeric values
SELECT DISTINCT
    product_id
FROM bronze.production_stocks
WHERE TRY_CAST(product_id AS INT) IS NULL AND product_id <> 'NULL';


----------------------
-- quantity column
----------------------

-- 1. Check for 'NULL' strings or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.production_stocks
WHERE quantity = 'NULL' OR TRIM(quantity) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.production_stocks
WHERE quantity IS NULL;

-- 3. Check non-numeric values
SELECT DISTINCT
    quantity
FROM bronze.production_stocks
WHERE TRY_CAST(quantity AS INT) IS NULL AND quantity <> 'NULL';

-- 4. Range check (negative or suspicious quantities)
SELECT 
    quantity, COUNT(*) AS cnt
FROM bronze.production_stocks
WHERE TRY_CAST(quantity AS INT) <= 0
GROUP BY quantity;

-- 5. Distribution summary
SELECT 
    MIN(TRY_CAST(quantity AS INT)) AS min_quantity,
    MAX(TRY_CAST(quantity AS INT)) AS max_quantity,
    COUNT(DISTINCT quantity) AS distinct_quantities
FROM bronze.production_stocks;


----------------------
-- Primary key / duplicates check
----------------------

-- Check if (store_id, product_id) combination is unique
SELECT 
    store_id, 
    product_id,
    COUNT(*) AS cnt
FROM bronze.production_stocks
GROUP BY store_id, product_id
HAVING COUNT(*) > 1;



--================
--SELECT staement
--================


SELECT
    TRY_CAST(NULLIF(store_id, 'NULL') AS INT)    AS store_id,
    TRY_CAST(NULLIF(product_id, 'NULL') AS INT)  AS product_id,
    TRY_CAST(NULLIF(quantity, 'NULL') AS INT)    AS quantity,

    -- Derived column: 1 if quantity = 0, else 0
    CAST(CASE 
        WHEN TRY_CAST(NULLIF(quantity, 'NULL') AS INT) = 0 THEN 1
        ELSE 0
    END AS BIT) AS out_of_stock

FROM bronze.production_stocks;
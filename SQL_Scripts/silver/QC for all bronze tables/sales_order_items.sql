/*
============================================================
 Data Quality Check Script - bronze.sales_order_items
============================================================
 Table       : bronze.sales_order_items
 Purpose     : Perform data quality checks on the order_items table 
               to identify dirty, inconsistent, or invalid data 
               before transforming into the Silver layer.
 

 Notes:
   - Apostrophes and hyphens in names are considered valid.
   - Results should guide cleaning rules for the Silver layer.
============================================================
*/
----------------------
--order_id column
----------------------

-- 1. Check for 'NULL' string or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.sales_order_items
WHERE order_id = 'NULL' OR LTRIM(RTRIM(order_id)) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.sales_order_items
WHERE order_id IS NULL;

-- 3. Check total distinct order_ids
SELECT 
    COUNT(DISTINCT order_id) AS distinct_orders,
    COUNT(*) AS total_rows
FROM bronze.sales_order_items;

-- 4. Check number of repeating order_id 
SELECT 
    order_id,
    COUNT(*) AS cnt
FROM bronze.sales_order_items
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

-- 5. Top 10 order_ids with most items
SELECT TOP 10
    order_id,
    COUNT(*) AS item_count
FROM bronze.sales_order_items
GROUP BY order_id
ORDER BY item_count DESC


----------------------
--item_id column
----------------------

-- 1. Check for 'NULL' strings or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.sales_order_items
WHERE item_id = 'NULL' OR LTRIM(RTRIM(item_id)) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.sales_order_items
WHERE item_id IS NULL;

-- 3. Check number of repeating order_id 
SELECT 
    item_id,
    COUNT(*) AS cnt
FROM bronze.sales_order_items
GROUP BY item_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;


-- 4. Check if (order_id, item_id) pairs are unique
SELECT 
    order_id, 
    item_id, 
    COUNT(*) AS cnt
FROM bronze.sales_order_items
GROUP BY order_id, item_id
HAVING COUNT(*) > 1;


----------------------
--product_id column
----------------------

-- 1. Check for 'NULL' strings or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.sales_order_items
WHERE product_id = 'NULL' OR LTRIM(RTRIM(product_id)) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.sales_order_items
WHERE product_id IS NULL;

-- 3. Check for non-numeric values
SELECT DISTINCT
    product_id
FROM bronze.sales_order_items
WHERE TRY_CAST(product_id AS INT) IS NULL
  AND product_id <> 'NULL';


-- 4. Check number of repeating order_id 
SELECT 
    product_id,
    COUNT(*) AS cnt
FROM bronze.sales_order_items
GROUP BY product_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;


-- 5. Range / distribution
SELECT 
    MIN(TRY_CAST(product_id AS INT)) AS min_product_id,
    MAX(TRY_CAST(product_id AS INT)) AS max_product_id,
    COUNT(DISTINCT product_id) AS distinct_products
FROM bronze.sales_order_items;


----------------------
--quantity column
----------------------

-- 1. Check for 'NULL' string or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.sales_order_items
WHERE quantity = 'NULL' OR LTRIM(RTRIM(quantity)) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.sales_order_items
WHERE quantity IS NULL;

-- 3. Check for non-numeric values
SELECT DISTINCT 
    quantity
FROM bronze.sales_order_items
WHERE TRY_CAST(quantity AS INT) IS NULL
  AND quantity <> 'NULL';

-- 4. Range check (min, max)
SELECT 
    MIN(TRY_CAST(quantity AS INT)) AS min_quantity,
    MAX(TRY_CAST(quantity AS INT)) AS max_quantity
FROM bronze.sales_order_items;

-- 5. Distribution of suspicious values (0 or negative)
SELECT 
    quantity, COUNT(*) AS cnt
FROM bronze.sales_order_items
WHERE TRY_CAST(quantity AS INT) <= 0
GROUP BY quantity;



----------------------
--list_price column
----------------------

-- 1. Check for 'NULL' string or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.sales_order_items
WHERE list_price = 'NULL' OR LTRIM(RTRIM(list_price)) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.sales_order_items
WHERE list_price IS NULL;

-- 3. Check for non-numeric values
SELECT DISTINCT
    list_price
FROM bronze.sales_order_items
WHERE TRY_CAST(list_price AS DECIMAL(10,2)) IS NULL
  AND list_price <> 'NULL';

-- 4. Range check (min, max)
SELECT 
    MIN(TRY_CAST(list_price AS DECIMAL(10,2))) AS min_price,
    MAX(TRY_CAST(list_price AS DECIMAL(10,2))) AS max_price
FROM bronze.sales_order_items;

-- 5. Check for suspicious values (like <= 0)
SELECT 
    list_price, COUNT(*) AS cnt
FROM bronze.sales_order_items
WHERE TRY_CAST(list_price AS DECIMAL(10,2)) <= 0
GROUP BY list_price;


----------------------
--discount column
----------------------

-- 1. Check for 'NULL' string or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.sales_order_items
WHERE discount = 'NULL' OR LTRIM(RTRIM(discount)) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.sales_order_items
WHERE discount IS NULL;

-- 3. Find non-numeric values
SELECT DISTINCT discount
FROM bronze.sales_order_items
WHERE TRY_CAST(discount AS DECIMAL(5,2)) IS NULL
  AND discount <> 'NULL';

-- 4. Range check (min, max)
SELECT 
    MIN(TRY_CAST(discount AS DECIMAL(5,2))) AS min_discount,
    MAX(TRY_CAST(discount AS DECIMAL(5,2))) AS max_discount
FROM bronze.sales_order_items;

-- 5. Suspicious discounts (negative or > 1)
SELECT discount, COUNT(*) AS cnt
FROM bronze.sales_order_items
WHERE TRY_CAST(discount AS DECIMAL(5,2)) < 0 
   OR TRY_CAST(discount AS DECIMAL(5,2)) > 1
GROUP BY discount;


--=================
--SELECT statement
--=================


SELECT
    CAST(NULLIF(order_id, 'NULL') AS INT) AS order_id,
    CAST(NULLIF(item_id, 'NULL') AS INT) AS item_id,
    CAST(NULLIF(product_id, 'NULL') AS INT) AS product_id,
    CAST(NULLIF(quantity, 'NULL') AS INT) AS quantity,
    CAST(NULLIF(list_price, 'NULL') AS DECIMAL(10,2)) AS list_price,
    CAST(NULLIF(discount, 'NULL') AS DECIMAL(5,2)) AS discount,
    -- Derived: discount percentage
    CAST(CAST(NULLIF(discount, 'NULL') AS DECIMAL(5,2)) * 100 AS DECIMAL(5,2)) AS discount_percentage,

    -- Derived: total price after discount
    CAST(
        CAST(NULLIF(quantity, 'NULL') AS INT) * 
        CAST(NULLIF(list_price, 'NULL') AS DECIMAL(10,2)) * 
        (1 - CAST(NULLIF(discount, 'NULL') AS DECIMAL(5,2)))
        AS DECIMAL(12,2)) AS total_price
FROM bronze.sales_order_items;

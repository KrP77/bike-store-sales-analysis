/*
============================================================
 Data Quality Check Script - bronze.sales_orders
============================================================
 Table       : bronze.sales_orders
 Purpose     : Perform data quality checks on each column 
               to identify dirty, inconsistent, or invalid data 
               before transforming into the Silver layer.
============================================================
*/

----------------------
-- order_id
----------------------

-- 1. Check for 'NULL' strings or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.sales_orders
WHERE order_id = 'NULL' OR LTRIM(RTRIM(order_id)) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.sales_orders
WHERE order_id IS NULL;

-- 3. Duplicates in order_id (should be unique PK)
SELECT 
    order_id, 
    COUNT(*) AS cnt
FROM bronze.sales_orders
GROUP BY order_id
HAVING COUNT(*) > 1;


----------------------
-- customer_id
----------------------

-- 1. Check for 'NULL' strings or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.sales_orders
WHERE customer_id = 'NULL' OR LTRIM(RTRIM(customer_id)) = '';

-- 2. Check for actual NULLs
SELECT 
    COUNT(*) AS null_count
FROM bronze.sales_orders
WHERE customer_id IS NULL;

-- 3. Spot invalid values (non-numeric)
SELECT DISTINCT
    customer_id
FROM bronze.sales_orders
WHERE TRY_CAST(customer_id AS INT) IS NULL AND customer_id <> 'NULL';


----------------------
-- order_status
----------------------

-- 1. Distinct values (should be small set)
SELECT DISTINCT
    order_status
FROM bronze.sales_orders;

-- 2. Check for 'NULL' strings or blanks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.sales_orders
WHERE order_status = 'NULL' OR LTRIM(RTRIM(order_status)) = '';

-- 3. Actual NULLs
SELECT
    COUNT(*) AS null_count
FROM bronze.sales_orders
WHERE order_status IS NULL;


----------------------
-- order_date
----------------------

-- 1. Check for invalid formats (should be dd-mm-yyyy)
SELECT DISTINCT 
    order_date
FROM bronze.sales_orders
WHERE TRY_CONVERT(DATE, order_date, 105) IS NULL AND order_date <> 'NULL';

-- 2. Check for NULLs
SELECT
    COUNT(*) AS null_string_count
FROM bronze.sales_orders
WHERE order_date = 'NULL' OR LTRIM(RTRIM(order_date)) = '';

SELECT 
    COUNT(*) AS null_count
FROM bronze.sales_orders
WHERE order_date IS NULL;


----------------------
-- required_date
----------------------

-- 1. Invalid formats
SELECT DISTINCT 
    required_date
FROM bronze.sales_orders
WHERE TRY_CONVERT(DATE, required_date, 105) IS NULL AND required_date <> 'NULL';

-- 2. NULL checks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.sales_orders
WHERE required_date = 'NULL' OR LTRIM(RTRIM(required_date)) = '';

SELECT
    COUNT(*) AS null_count
FROM bronze.sales_orders
WHERE required_date IS NULL;


----------------------
-- shipped_date
----------------------

-- 1. Invalid formats
SELECT DISTINCT
    shipped_date
FROM bronze.sales_orders
WHERE TRY_CONVERT(DATE, shipped_date, 105) IS NULL AND shipped_date <> 'NULL';

-- 2. NULL checks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.sales_orders
WHERE shipped_date = 'NULL' OR LTRIM(RTRIM(shipped_date)) = '';

-- convert string NULL in actual SQL NULL
SELECT 
    TRY_CONVERT(DATE, NULLIF(shipped_date, 'NULL'), 105) AS shipped_date
FROM bronze.sales_orders;


SELECT 
    COUNT(*) AS null_count
FROM bronze.sales_orders
WHERE shipped_date IS NULL;

-- 3. Business rule check: shipped_date >= order_date
SELECT 
    order_id,
    order_date, 
    shipped_date
FROM bronze.sales_orders
WHERE TRY_CONVERT(DATE, shipped_date, 105) < TRY_CONVERT(DATE, order_date, 105);


----------------------
-- store_id
----------------------

-- 1. NULL checks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.sales_orders
WHERE store_id = 'NULL' OR LTRIM(RTRIM(store_id)) = '';

SELECT 
    COUNT(*) AS null_count
FROM bronze.sales_orders
WHERE store_id IS NULL;

-- 2. Invalid (non-numeric)
SELECT DISTINCT 
    store_id
FROM bronze.sales_orders
WHERE TRY_CAST(store_id AS INT) IS NULL AND store_id <> 'NULL';


----------------------
-- staff_id
----------------------

-- 1. NULL checks
SELECT 
    COUNT(*) AS null_string_count
FROM bronze.sales_orders
WHERE staff_id = 'NULL' OR LTRIM(RTRIM(staff_id)) = '';

SELECT 
    COUNT(*) AS null_count
FROM bronze.sales_orders
WHERE staff_id IS NULL;

-- 2. Invalid (non-numeric)
SELECT DISTINCT
    staff_id
FROM bronze.sales_orders
WHERE TRY_CAST(staff_id AS INT) IS NULL AND staff_id <> 'NULL';



--=======================
--SELECT statement
--=======================


SELECT
    -- Convert IDs to INT, handle 'NULL' strings
    TRY_CONVERT(INT, NULLIF(order_id, 'NULL'))       AS order_id,
    TRY_CONVERT(INT, NULLIF(customer_id, 'NULL'))    AS customer_id,
    TRY_CONVERT(INT, NULLIF(order_status, 'NULL'))   AS order_status,
    TRY_CONVERT(INT, NULLIF(store_id, 'NULL'))       AS store_id,
    TRY_CONVERT(INT, NULLIF(staff_id, 'NULL'))       AS staff_id,

    -- Convert dates (format: DD-MM-YYYY = 105 style), handle 'NULL'
    TRY_CONVERT(DATE, NULLIF(order_date, 'NULL'), 105)     AS order_date,
    TRY_CONVERT(DATE, NULLIF(required_date, 'NULL'), 105)  AS required_date,
    TRY_CONVERT(DATE, NULLIF(shipped_date, 'NULL'), 105)   AS shipped_date,

    -- Derived: ship status
    CASE 
        WHEN TRY_CONVERT(DATE, NULLIF(shipped_date, 'NULL'), 105) IS NULL 
            THEN 'Pending'
        ELSE 'Completed'
    END AS ship_status,

    -- Derived: shipping duration
    DATEDIFF(DAY, 
        TRY_CONVERT(DATE, NULLIF(order_date, 'NULL'), 105), 
        TRY_CONVERT(DATE, NULLIF(shipped_date, 'NULL'), 105)
    ) AS days_to_ship,

    -- Derived: delay vs required date
    DATEDIFF(DAY, 
        TRY_CONVERT(DATE, NULLIF(required_date, 'NULL'), 105), 
        TRY_CONVERT(DATE, NULLIF(shipped_date, 'NULL'), 105)
    ) AS delay_vs_required



FROM bronze.sales_orders;
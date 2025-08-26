/*
============================================================
 Data Quality Check Script - bronze.sales_staffs
============================================================
 Table       : bronze.sales_staffs
 Purpose     : Perform data quality checks on the staffs table 
               to identify dirty, inconsistent, or invalid data 
               before transforming into the Silver layer.
 
 Notes:
   - Apostrophes and hyphens in names are considered valid.
   - Phone format expected with brackets and dashes (US style).
   - Manager hierarchy will be validated against staff_id values.
============================================================
*/

----------------------
--staff_id column
----------------------

-- 1. Check duplicate staff_id
SELECT
    staff_id,
    COUNT(*) AS cnt
FROM bronze.sales_staffs
GROUP BY staff_id
HAVING COUNT(*) > 1;

-- 2. Check for null/placeholder staff_id
SELECT 
    COUNT(*) AS null_count
FROM bronze.sales_staffs
WHERE staff_id = 'NULL' OR staff_id IS NULL;


----------------------
--first_name column
----------------------

-- 1. Count null/blank/placeholder names
SELECT 
    COUNT(*) AS null_first_name
FROM bronze.sales_staffs
WHERE first_name = 'NULL' OR TRIM(first_name) = '';

-- 2. Check invalid characters (digits or special chars)
SELECT DISTINCT
    first_name
FROM bronze.sales_staffs
WHERE first_name LIKE '%[0-9]%' OR first_name LIKE '%[^a-zA-Z -]%';

-- 3. Check longest names
SELECT TOP 10
    first_name, 
    LEN(first_name) AS length
FROM bronze.sales_staffs
ORDER BY LEN(first_name) DESC;


----------------------
--last_name column
----------------------

-- 1. Count null/blank/placeholder names
SELECT 
    COUNT(*) AS null_last_name
FROM bronze.sales_staffs
WHERE last_name = 'NULL' OR TRIM(last_name) = '';

-- 2. Check invalid characters (digits or special chars)
SELECT DISTINCT 
    last_name
FROM bronze.sales_staffs
WHERE last_name LIKE '%[0-9]%' OR last_name LIKE '%[^a-zA-Z -]%';

-- 3. Check longest names
SELECT TOP 10 
    last_name,
    LEN(last_name) AS length
FROM bronze.sales_staffs
ORDER BY LEN(last_name) DESC;


----------------------
--first_name + last_name combo
----------------------

-- 1. Detect duplicate full names
SELECT 
    first_name,
    last_name,
    COUNT(*) AS cnt
FROM bronze.sales_staffs
GROUP BY first_name, last_name
HAVING COUNT(*) > 1;


----------------------
--email column
----------------------

-- 1. Count NULL / blank emails
SELECT 
    COUNT(*) AS null_email_count
FROM bronze.sales_staffs
WHERE email = 'NULL' OR TRIM(email) = '';

-- 2. Emails without '@' or '.'
SELECT DISTINCT 
    email
FROM bronze.sales_staffs
WHERE email NOT LIKE '%@%' OR email NOT LIKE '%.%';

-- 3. Emails with spaces (invalid)
SELECT DISTINCT 
    email
FROM bronze.sales_staffs
WHERE email LIKE '% %';

-- 4. Duplicate emails
SELECT 
    email,
    COUNT(*) AS cnt
FROM bronze.sales_staffs
WHERE email <> 'NULL'
GROUP BY email
HAVING COUNT(*) > 1;

-- 5. Distribution of email lengths
SELECT 
    LEN(email) AS length,
    COUNT(*) AS cnt
FROM bronze.sales_staffs
WHERE email <> 'NULL'
GROUP BY LEN(email)
ORDER BY cnt DESC;


----------------------
--phone column
----------------------

-- 1. Check for NULL / placeholder
SELECT 
    COUNT(*) AS null_phone_count
FROM bronze.sales_staffs
WHERE phone = 'NULL' OR phone IS NULL;

-- 2. Detect duplicates
SELECT 
    phone,
    COUNT(*) AS cnt
FROM bronze.sales_staffs
WHERE phone <> 'NULL'
GROUP BY phone
HAVING COUNT(*) > 1;

-- 3. Check invalid characters
SELECT DISTINCT
    phone
FROM bronze.sales_staffs
WHERE phone NOT LIKE '(%[0-9][0-9][0-9]%) %[0-9][0-9][0-9]%-[0-9][0-9][0-9][0-9]';

-- 4. Check unusual lengths
SELECT 
    phone,
    LEN(phone) AS length
FROM bronze.sales_staffs
WHERE phone <> 'NULL'
ORDER BY LEN(phone);


----------------------
--active column
----------------------

-- 1. Profile active column values
SELECT 
    active, 
    COUNT(*) AS cnt
FROM bronze.sales_staffs
GROUP BY active;

-- 2. Detect invalid values (expected 0 or 1)
SELECT DISTINCT 
    active
FROM bronze.sales_staffs
WHERE TRY_CAST(active AS INT) NOT IN (0,1);


----------------------
--store_id column
----------------------

-- 1. Count NULLs
SELECT 
    COUNT(*) AS null_store_count
FROM bronze.sales_staffs
WHERE store_id = 'NULL' OR store_id IS NULL;

-- 2. Profile distinct stores
SELECT 
    store_id,
    COUNT(*) AS cnt
FROM bronze.sales_staffs
GROUP BY store_id;


----------------------
--manager_id column
----------------------

-- 1. Count NULL managers
SELECT 
    COUNT(*) AS null_manager_count
FROM bronze.sales_staffs
WHERE manager_id = 'NULL' OR manager_id IS NULL;

-- 2. Profile distinct manager_ids
SELECT 
    manager_id,
    COUNT(*) AS cnt
FROM bronze.sales_staffs
GROUP BY manager_id;

-- 3. Detect invalid manager references (not present in staff_id list)
SELECT DISTINCT 
    manager_id
FROM bronze.sales_staffs
WHERE manager_id IS NOT NULL
  AND manager_id <> 'NULL'
  AND manager_id NOT IN (SELECT staff_id FROM bronze.sales_staffs);



--====================
--SELECT statement
--====================



SELECT
    -- staff_id: int
    TRY_CAST(NULLIF(TRIM(staff_id), 'NULL') AS INT) AS staff_id,

    -- first_name & last_name
    NULLIF(TRIM(first_name), 'NULL') AS first_name,
    NULLIF(TRIM(last_name), 'NULL') AS last_name,

    -- email
    NULLIF(TRIM(email), 'NULL') AS email,

    -- phone: remove brackets, hyphens, spaces; keep only digits
    CAST
    (
        CASE 
            WHEN phone = 'NULL' THEN NULL
            ELSE REPLACE(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), '-', ''), ' ', '')
        END
        AS VARCHAR(10)
    ) AS phone,

    -- active
    TRY_CAST(NULLIF(TRIM(active), 'NULL') AS INT) AS active,

    -- store_id
    TRY_CAST(NULLIF(TRIM(store_id), 'NULL') AS INT) AS store_id,

    -- manager_id: NULL for top manager
    TRY_CAST(NULLIF(TRIM(manager_id), 'NULL') AS INT) AS manager_id

FROM bronze.sales_staffs;

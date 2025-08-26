/*
============================================================
 Data Quality Check Script - bronze.sales_stores
============================================================
 Table       : bronze.sales_stores
 Purpose     : Perform data quality checks on the stores table 
               to identify dirty, inconsistent, or invalid data 
               before transforming into the Silver layer.
 
 Notes:
   - Apostrophes and hyphens in names are considered valid.
   - Phone format expected with brackets and dashes (US style).
   - Manager hierarchy will be validated against staff_id values.
============================================================
*/

SELECT
    -- store_id
    TRY_CAST(NULLIF(TRIM(store_id), 'NULL') AS INT) AS store_id,

    -- store_name
    NULLIF(TRIM(store_name), 'NULL') AS store_name,

    -- phone: remove brackets, hyphens, spaces
    CAST
    (
        CASE 
            WHEN phone = 'NULL' THEN NULL
            ELSE REPLACE(REPLACE(REPLACE(REPLACE(phone, '(', ''), ')', ''), '-', ''), ' ', '')
        END 
        AS VARCHAR(10)
    ) AS phone,

    -- email
    NULLIF(TRIM(email), 'NULL') AS email,

    -- address details
    NULLIF(TRIM(street), 'NULL') AS street,
    NULLIF(TRIM(city), 'NULL') AS city,

    -- state: always 2-char code
    CAST(NULLIF(TRIM(state), 'NULL') AS VARCHAR(2)) AS state,

    -- zip_code: always 5-char
    CAST(NULLIF(TRIM(zip_code), 'NULL') AS VARCHAR(5)) AS zip_code,

    -- dervied : full address
    CAST(NULLIF(TRIM(street), 'NULL') + ', ' + NULLIF(TRIM(city), 'NULL') + ', ' + CAST(NULLIF(TRIM(state), 'NULL') AS VARCHAR(2)) + ', ' + CAST(NULLIF(TRIM(zip_code), 'NULL') AS VARCHAR(5)) AS VARCHAR(370)) AS full_address


FROM bronze.sales_stores;

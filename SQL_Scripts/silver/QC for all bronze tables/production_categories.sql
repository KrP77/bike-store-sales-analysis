/*
============================================================
 Data Quality Check Script - bronze.production_categories
============================================================
 Table       : bronze.production_categories
 Purpose     : Perform data quality checks on the production_categories table 
               to identify dirty, inconsistent, or invalid data 
               before transforming into the Silver layer.
 

 Notes:
   - Apostrophes and hyphens in names are considered valid.
   - Results should guide cleaning rules for the Silver layer.
============================================================
*/

--================
--SELECT staement
--================
SELECT
    -- category_id: INT
    TRY_CAST(NULLIF(TRIM(category_id), 'NULL') AS INT) AS category_id,

    -- category_name
    NULLIF(TRIM(category_name), 'NULL') AS category_name

FROM bronze.production_categories;

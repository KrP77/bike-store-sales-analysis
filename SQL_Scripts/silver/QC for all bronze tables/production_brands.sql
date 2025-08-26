/*
============================================================
 Data Quality Check Script - bronze.production_brands
============================================================
 Table       : bronze.production_brands
 Purpose     : Perform data quality checks on the production_brands table 
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
    -- brand_id: convert to INT, handle 'NULL' if any
    TRY_CAST(NULLIF(TRIM(brand_id), 'NULL') AS INT) AS brand_id,

    -- brand_name: remove leading/trailing spaces, replace 'NULL' string with actual NULL
    NULLIF(TRIM(brand_name), 'NULL') AS brand_name

FROM bronze.production_brands;

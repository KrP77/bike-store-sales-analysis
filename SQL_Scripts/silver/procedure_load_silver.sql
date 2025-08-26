/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
This stored procedure loads data into the 'silver' schema from bronze tables. 
It performs the following actions:
  - Truncates the silver tables before loading data.
  - Uses INSERT INTO ... SELECT ... for data transformation and loading.
  - Casts types, cleans data, and adds derived columns.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '=============================================';
        PRINT 'Loading the silver layer';
        PRINT '=============================================';

        -- =============================================================
        -- Load Sales Tables
        -- =============================================================
        PRINT '------------------------------------';
        PRINT 'Loading Sales tables';
        PRINT '------------------------------------';


        -- silver.sales_customers
        SET @start_time = GETDATE();
        PRINT '>> Truncating table : silver.sales_customers';
        TRUNCATE TABLE silver.sales_customers;

        PRINT '>> Inserting into table : silver.sales_customers';
        INSERT INTO silver.sales_customers
        (
            customer_id,
            first_name,
            last_name,
            phone,
            email,
            street, 
            city,
            state,
            zip_code,
            full_address
        )
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
        SET @end_time = GETDATE();
        PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';






        -- silver.sales_staffs
        SET @start_time = GETDATE();
        PRINT '>> Truncating table : silver.sales_staffs';
        TRUNCATE TABLE silver.sales_staffs;

        PRINT '>> Inserting into table : silver.sales_staffs';
        INSERT INTO silver.sales_staffs
        (
            staff_id,
            first_name, 
            last_name,
            email, 
            phone, 
            active,
            store_id, 
            manager_id
        )
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

        SET @end_time = GETDATE();
        PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';




        -- silver.sales_stores
        SET @start_time = GETDATE();
        PRINT '>> Truncating table : silver.sales_stores';
        TRUNCATE TABLE silver.sales_stores;

        PRINT '>> Inserting into table : silver.sales_stores';
        INSERT INTO silver.sales_stores
        (
            store_id,
            store_name,
            phone,
            email,
            street,
            city,
            state,
            zip_code, 
            full_address
        )
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
        SET @end_time = GETDATE();
        PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';





        -- silver.sales_orders
        SET @start_time = GETDATE();
        PRINT '>> Truncating table : silver.sales_orders';
        TRUNCATE TABLE silver.sales_orders;

        PRINT '>> Inserting into table : silver.sales_orders';
        INSERT INTO silver.sales_orders
        (
            order_id,
            customer_id,
            order_status, 
            store_id, 
            staff_id,
            order_date, 
            required_date,
            shipped_date,
            ship_status, 
            days_to_ship,
            delay_vs_required
        )
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
        SET @end_time = GETDATE();
        PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';




        -- silver.sales_order_items
        SET @start_time = GETDATE();
        PRINT '>> Truncating table : silver.sales_order_items';
        TRUNCATE TABLE silver.sales_order_items;

        PRINT '>> Inserting into table : silver.sales_order_items';
        INSERT INTO silver.sales_order_items
        (
            order_id,
            item_id,
            product_id,
            quantity,
            list_price,
            discount, 
            discount_percentage, 
            total_price
        )
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
        SET @end_time = GETDATE();
        PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        -- =============================================================
        -- Load Production Tables
        -- =============================================================
        PRINT '------------------------------------';
        PRINT 'Loading Production tables';
        PRINT '------------------------------------';

        -- silver.production_categories
        SET @start_time = GETDATE();
        PRINT '>> Truncating table : silver.production_categories';
        TRUNCATE TABLE silver.production_categories;

        PRINT '>> Inserting into table : silver.production_categories';
        INSERT INTO silver.production_categories
        (
            category_id,
            category_name
        )
        SELECT
            -- category_id: INT
            TRY_CAST(NULLIF(TRIM(category_id), 'NULL') AS INT) AS category_id,

            -- category_name
            NULLIF(TRIM(category_name), 'NULL') AS category_name

        FROM bronze.production_categories;
        SET @end_time = GETDATE();
        PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';





        -- silver.production_brands
        SET @start_time = GETDATE();
        PRINT '>> Truncating table : silver.production_brands';
        TRUNCATE TABLE silver.production_brands;

        PRINT '>> Inserting into table : silver.production_brands';
        INSERT INTO silver.production_brands
        (
            brand_id, 
            brand_name
        )
        SELECT
            -- brand_id: convert to INT, handle 'NULL' if any
            TRY_CAST(NULLIF(TRIM(brand_id), 'NULL') AS INT) AS brand_id,

            -- brand_name: remove leading/trailing spaces, replace 'NULL' string with actual NULL
            NULLIF(TRIM(brand_name), 'NULL') AS brand_name

        FROM bronze.production_brands;

        SET @end_time = GETDATE();
        PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';





        -- silver.production_products
        SET @start_time = GETDATE();
        PRINT '>> Truncating table : silver.production_products';
        TRUNCATE TABLE silver.production_products;

        PRINT '>> Inserting into table : silver.production_products';
        INSERT INTO silver.production_products
        (
            product_id,
            product_name,
            brand_id,
            category_id,
            model_year,
            list_price
        )
        SELECT
            CAST(NULLIF(product_id, 'NULL') AS INT) AS product_id,
    
            -- Remove double quotes and trim
            REPLACE(NULLIF(TRIM(product_name), 'NULL'), '"', '') AS product_name,
    
            CAST(NULLIF(brand_id, 'NULL') AS INT) AS brand_id,
            CAST(NULLIF(category_id, 'NULL') AS INT) AS category_id,
            CAST(NULLIF(model_year, 'NULL') AS INT) AS model_year,
            CAST(NULLIF(list_price, 'NULL') AS DECIMAL(10,2)) AS list_price
        FROM bronze.production_products;

        SET @end_time = GETDATE();
        PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';





        -- silver.production_stocks
        SET @start_time = GETDATE();
        PRINT '>> Truncating table : silver.production_stocks';
        TRUNCATE TABLE silver.production_stocks;

        PRINT '>> Inserting into table : silver.production_stocks';
        INSERT INTO silver.production_stocks
        (
            store_id, 
            product_id,
            quantity,
            out_of_stock
        )
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
        SET @end_time = GETDATE();
        PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        -- =============================================================
        -- Completion
        -- =============================================================
        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==========================================';

    END TRY
    BEGIN CATCH
        PRINT '============================';
        PRINT 'Error occurred while loading the Silver layer';
        PRINT 'Error message : ' + ERROR_MESSAGE();
        PRINT 'Error number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error state : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '============================';
    END CATCH
END;
GO

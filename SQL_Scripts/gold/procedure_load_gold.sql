/*
===============================================================================
Stored Procedure: Load Gold Layer (Silver -> Gold)
===============================================================================
This stored procedure loads data into the 'gold' schema from silver tables. 
It performs the following actions:
  - Truncates the gold tables before loading data.
  - Uses INSERT INTO ... SELECT ... for data transformation and loading.
  - Joins.

Parameters:
    None. 
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC gold.load_gold;
*/

CREATE OR ALTER PROCEDURE gold.load_gold AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '============================================='
        PRINT 'Loading the Gold Layer'
        PRINT '============================================='


        -- =============================================================
        -- Load Dimension Tables
        -- =============================================================
        PRINT '------------------------------------'
        PRINT 'Loading Dimension tables'
        PRINT '------------------------------------'


        -- gold.dim_customers
        SET @start_time = GETDATE();
        PRINT '>> Truncating table : gold.dim_customers';
        TRUNCATE TABLE gold.dim_customers;

        PRINT '>> Inserting into table : gold.dim_customers';
        INSERT INTO gold.dim_customers
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
        FROM silver.sales_customers;
        SET @end_time = GETDATE();
        PRINT '>> Load duration (dim_customers): ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';



        -- gold.dim_staffs
        SET @start_time = GETDATE();
        PRINT '>> Truncating table : gold.dim_staffs';
        TRUNCATE TABLE gold.dim_staffs;

        PRINT '>> Inserting into table : gold.dim_staffs';
        INSERT INTO gold.dim_staffs
        (
            staff_id,
            first_name,
            last_name,
            phone,
            email,
            active,
            manager_id,
            store_id
        )
        SELECT
            staff_id,
            first_name,
            last_name,
            phone,
            email,
            active,
            manager_id,
            store_id
        FROM silver.sales_staffs;
        SET @end_time = GETDATE();
        PRINT '>> Load duration (dim_staffs): ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';



        -- gold.dim_stores
        SET @start_time = GETDATE();
        PRINT '>> Truncating table : gold.dim_stores';
        TRUNCATE TABLE gold.dim_stores;

        PRINT '>> Inserting into table : gold.dim_stores';
        INSERT INTO gold.dim_stores
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
            store_id,
            store_name,
            phone,
            email,
            street,
            city,
            state,
            zip_code,
            full_address
        FROM silver.sales_stores;
        SET @end_time = GETDATE();
        PRINT '>> Load duration (dim_stores): ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';



        -- gold.dim_products
        SET @start_time = GETDATE();
        PRINT '>> Truncating table : gold.dim_products';
        TRUNCATE TABLE gold.dim_products;

        PRINT '>> Inserting into table : gold.dim_products';
        INSERT INTO gold.dim_products
        (
            product_id,
            category_id,
            brand_id,
            product_name,
            category_name,
            brand_name,
            model_year,
            list_price
        )
        SELECT
            p.product_id,
            c.category_id,
            b.brand_id,
            p.product_name,
            c.category_name,
            b.brand_name,
            p.model_year,
            p.list_price
        FROM silver.production_products AS p
        JOIN silver.production_categories AS c
            ON p.category_id = c.category_id
        JOIN silver.production_brands AS b
            ON p.brand_id = b.brand_id;
        SET @end_time = GETDATE();
        PRINT '>> Load duration (dim_products): ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';



        -- =============================================================
        -- Load Fact Tables
        -- =============================================================
        PRINT '------------------------------------'
        PRINT 'Loading Fact tables'
        PRINT '------------------------------------'


        -- gold.fact_sales
        SET @start_time = GETDATE();
        PRINT '>> Truncating table : gold.fact_sales';
        TRUNCATE TABLE gold.fact_sales;

        PRINT '>> Inserting into table : gold.fact_sales';
        INSERT INTO gold.fact_sales
        (
            order_id,
            item_id,
            customer_id,
            product_id,
            staff_id,
            store_id,
            quantity,
            list_price,
            discount,
            discount_percentage,
            total_price,
            order_status,
            order_status_desc,
            order_date,
            required_date,
            shipped_date,
            ship_status,
            days_to_ship,
            delay_vs_required
        )
        SELECT
            o.order_id,
            oi.item_id,
            o.customer_id,
            oi.product_id,
            o.staff_id,
            o.store_id,
            oi.quantity,
            oi.list_price,
            oi.discount,
            oi.discount_percentage,
            oi.total_price,
            o.order_status,
            o.order_status_desc,
            o.order_date,
            o.required_date,
            o.shipped_date,
            o.ship_status,
            o.days_to_ship,
            o.delay_vs_required
        FROM silver.sales_orders AS o
        JOIN silver.sales_order_items AS oi 
            ON o.order_id = oi.order_id;
        SET @end_time = GETDATE();
        PRINT '>> Load duration (fact_sales): ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';



        -- gold.fact_stocks
        SET @start_time = GETDATE();
        PRINT '>> Truncating table : gold.fact_stocks';
        TRUNCATE TABLE gold.fact_stocks;

        PRINT '>> Inserting into table : gold.fact_stocks';
        INSERT INTO gold.fact_stocks
        (
            store_id,
            product_id,
            quantity,
            out_of_stock
        )
        SELECT
            store_id,
            product_id,
            quantity,
            out_of_stock
        FROM silver.production_stocks;
        SET @end_time = GETDATE();
        PRINT '>> Load duration (fact_stocks): ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';



        -- =============================================================
        -- Completion
        -- =============================================================
        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Loading Gold Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==========================================';

    END TRY
    BEGIN CATCH
        PRINT '============================';
        PRINT 'Error occurred while loading the Gold layer';
        PRINT 'Error message : ' + ERROR_MESSAGE();
        PRINT 'Error number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error state : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '============================';
    END CATCH
END;
GO

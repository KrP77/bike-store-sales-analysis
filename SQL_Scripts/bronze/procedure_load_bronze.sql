/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
This stored procedure loads data into the 'bronze' schema from external CSV files. 
It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=============================================';
		PRINT 'Loading the bronze layer';
		PRINT '=============================================';
	
		-- =============================================================
		-- Load Sales Tables
		-- =============================================================
		PRINT '------------------------------------';
		PRINT 'Loading Sales tables';
		PRINT '------------------------------------';

		-- bronze.sales_customers
		SET @start_time = GETDATE();
		PRINT '>> Truncating table : bronze.sales_customers';
		TRUNCATE TABLE bronze.sales_customers;

		PRINT '>> Bulk inserting into table : bronze.sales_customers';
		BULK INSERT bronze.sales_customers
		FROM 'C:\your_path\Dataset\customers.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';


		-- bronze.sales_staffs
		SET @start_time = GETDATE();
		PRINT '>> Truncating table : bronze.sales_staffs';
		TRUNCATE TABLE bronze.sales_staffs;

		PRINT '>> Bulk inserting into table : bronze.sales_staffs';
		BULK INSERT bronze.sales_staffs
		FROM 'C:\your_path\Dataset\staffs.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';


		-- bronze.sales_orders
		SET @start_time = GETDATE();
		PRINT '>> Truncating table : bronze.sales_orders';
		TRUNCATE TABLE bronze.sales_orders;

		PRINT '>> Bulk inserting into table : bronze.sales_orders';
		BULK INSERT bronze.sales_orders
		FROM 'C:\your_path\Dataset\orders.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';


		-- bronze.sales_stores
		SET @start_time = GETDATE();
		PRINT '>> Truncating table : bronze.sales_stores';
		TRUNCATE TABLE bronze.sales_stores;

		PRINT '>> Bulk inserting into table : bronze.sales_stores';
		BULK INSERT bronze.sales_stores
		FROM 'C:\your_path\Dataset\stores.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';


		-- bronze.sales_order_items
		SET @start_time = GETDATE();
		PRINT '>> Truncating table : bronze.sales_order_items';
		TRUNCATE TABLE bronze.sales_order_items;

		PRINT '>> Bulk inserting into table : bronze.sales_order_items';
		BULK INSERT bronze.sales_order_items
		FROM 'C:\your_path\Dataset\order_items.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';



		-- =============================================================
		-- Load Production Tables
		-- =============================================================
		PRINT '------------------------------------';
		PRINT 'Loading Production tables';
		PRINT '------------------------------------';

		-- bronze.production_categories
		SET @start_time = GETDATE();
		PRINT '>> Truncating table : bronze.production_categories';
		TRUNCATE TABLE bronze.production_categories;

		PRINT '>> Bulk inserting into table : bronze.production_categories';
		BULK INSERT bronze.production_categories
		FROM 'C:\your_path\Dataset\categories.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';


		-- bronze.production_products
		SET @start_time = GETDATE();
		PRINT '>> Truncating table : bronze.production_products';
		TRUNCATE TABLE bronze.production_products;

		PRINT '>> Bulk inserting into table : bronze.production_products';
		BULK INSERT bronze.production_products
		FROM 'C:\your_path\Dataset\products.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';


		-- bronze.production_stocks
		SET @start_time = GETDATE();
		PRINT '>> Truncating table : bronze.production_stocks';
		TRUNCATE TABLE bronze.production_stocks;

		PRINT '>> Bulk inserting into table : bronze.production_stocks';
		BULK INSERT bronze.production_stocks
		FROM 'C:\your_path\Dataset\stocks.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';


		-- bronze.production_brands
		SET @start_time = GETDATE();
		PRINT '>> Truncating table : bronze.production_brands';
		TRUNCATE TABLE bronze.production_brands;

		PRINT '>> Bulk inserting into table : bronze.production_brands';
		BULK INSERT bronze.production_brands
		FROM 'C:\your_path\Dataset\brands.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ----------------';


		-- =============================================================
		-- Completion
		-- =============================================================
		SET @batch_end_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================================';


	END TRY
	BEGIN CATCH
		PRINT '============================';
		PRINT 'Error occured while loading the bronze layer';
		PRINT 'Error message : ' + ERROR_MESSAGE();
		PRINT 'Error number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error state' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '============================';
	END CATCH
END;

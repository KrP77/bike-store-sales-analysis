/*
==================================================
DDL Script : Create Bronze Tables for Bike Store Dataset
==================================================
This script creates the bronze tables from the raw CSV source files.
It creates them in the bronze schema. If they already exist, they will be dropped first.
==================================================
*/

-- SALES TABLES ---------------------------------------------------

-- Customers
IF OBJECT_ID('bronze.sales_customers', 'U') IS NOT NULL
    DROP TABLE bronze.sales_customers;
GO

CREATE TABLE bronze.sales_customers (
    customer_id     NVARCHAR(50),
    first_name      NVARCHAR(100),
    last_name       NVARCHAR(100),
    phone           NVARCHAR(50),
    email           NVARCHAR(255),
    street          NVARCHAR(255),
    city            NVARCHAR(100),
    state           NVARCHAR(50),
    zip_code        NVARCHAR(20)
);
GO

-- Staffs
IF OBJECT_ID('bronze.sales_staffs', 'U') IS NOT NULL
    DROP TABLE bronze.sales_staffs;
GO

CREATE TABLE bronze.sales_staffs (
    staff_id        NVARCHAR(50),
    first_name      NVARCHAR(100),
    last_name       NVARCHAR(100),
    email           NVARCHAR(255),
    phone           NVARCHAR(50),
    active          NVARCHAR(10),
    store_id        NVARCHAR(50),
    manager_id      NVARCHAR(50)
);
GO

-- Stores
IF OBJECT_ID('bronze.sales_stores', 'U') IS NOT NULL
    DROP TABLE bronze.sales_stores;
GO

CREATE TABLE bronze.sales_stores (
    store_id        NVARCHAR(50),
    store_name      NVARCHAR(255),
    phone           NVARCHAR(50),
    email           NVARCHAR(255),
    street          NVARCHAR(255),
    city            NVARCHAR(100),
    state           NVARCHAR(50),
    zip_code        NVARCHAR(20)
);
GO

-- Orders (dates kept as NVARCHAR for raw storage)
IF OBJECT_ID('bronze.sales_orders', 'U') IS NOT NULL
    DROP TABLE bronze.sales_orders;
GO

CREATE TABLE bronze.sales_orders (
    order_id        NVARCHAR(50),
    customer_id     NVARCHAR(50),
    order_status    NVARCHAR(50),
    order_date      NVARCHAR(50),  -- was DATE
    required_date   NVARCHAR(50),  -- was DATE
    shipped_date    NVARCHAR(50),  -- was DATE
    store_id        NVARCHAR(50),
    staff_id        NVARCHAR(50)
);
GO

-- Order Items
IF OBJECT_ID('bronze.sales_order_items', 'U') IS NOT NULL
    DROP TABLE bronze.sales_order_items;
GO

CREATE TABLE bronze.sales_order_items (
    order_id        NVARCHAR(50),
    item_id         NVARCHAR(50),
    product_id      NVARCHAR(50),
    quantity        NVARCHAR(50),
    list_price      NVARCHAR(50),
    discount        NVARCHAR(50)
);
GO


-- PRODUCTION TABLES ---------------------------------------------

-- Categories
IF OBJECT_ID('bronze.production_categories', 'U') IS NOT NULL
    DROP TABLE bronze.production_categories;
GO

CREATE TABLE bronze.production_categories (
    category_id     NVARCHAR(50),
    category_name   NVARCHAR(100)
);
GO

-- Brands
IF OBJECT_ID('bronze.production_brands', 'U') IS NOT NULL
    DROP TABLE bronze.production_brands;
GO

CREATE TABLE bronze.production_brands (
    brand_id        NVARCHAR(50),
    brand_name      NVARCHAR(100)
);
GO

-- Products
IF OBJECT_ID('bronze.production_products', 'U') IS NOT NULL
    DROP TABLE bronze.production_products;
GO

CREATE TABLE bronze.production_products (
    product_id      NVARCHAR(50),
    product_name    NVARCHAR(255),
    brand_id        NVARCHAR(50),
    category_id     NVARCHAR(50),
    model_year      NVARCHAR(50),
    list_price      NVARCHAR(50)
);
GO

-- Stocks
IF OBJECT_ID('bronze.production_stocks', 'U') IS NOT NULL
    DROP TABLE bronze.production_stocks;
GO

CREATE TABLE bronze.production_stocks (
    store_id        NVARCHAR(50),
    product_id      NVARCHAR(50),
    quantity        NVARCHAR(50)
);
GO

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
    customer_id     INT,
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
    staff_id        INT,
    first_name      NVARCHAR(100),
    last_name       NVARCHAR(100),
    email           NVARCHAR(255),
    phone           NVARCHAR(50),
    active          BIT,
    store_id        INT,
    manager_id      INT
);
GO

-- Stores
IF OBJECT_ID('bronze.sales_stores', 'U') IS NOT NULL
    DROP TABLE bronze.sales_stores;
GO

CREATE TABLE bronze.sales_stores (
    store_id        INT,
    store_name      NVARCHAR(255),
    phone           NVARCHAR(50),
    email           NVARCHAR(255),
    street          NVARCHAR(255),
    city            NVARCHAR(100),
    state           NVARCHAR(50),
    zip_code        NVARCHAR(20)
);
GO

-- Orders
IF OBJECT_ID('bronze.sales_orders', 'U') IS NOT NULL
    DROP TABLE bronze.sales_orders;
GO

CREATE TABLE bronze.sales_orders (
    order_id        INT,
    customer_id     INT,
    order_status    INT,
    order_date      DATE,
    required_date   DATE,
    shipped_date    DATE,
    store_id        INT,
    staff_id        INT
);
GO

-- Order Items
IF OBJECT_ID('bronze.sales_order_items', 'U') IS NOT NULL
    DROP TABLE bronze.sales_order_items;
GO

CREATE TABLE bronze.sales_order_items (
    order_id        INT,
    item_id         INT,
    product_id      INT,
    quantity        INT,
    list_price      DECIMAL(10,2),
    discount        DECIMAL(5,2)
);
GO


-- PRODUCTION TABLES ---------------------------------------------

-- Categories
IF OBJECT_ID('bronze.production_categories', 'U') IS NOT NULL
    DROP TABLE bronze.production_categories;
GO

CREATE TABLE bronze.production_categories (
    category_id     INT,
    category_name   NVARCHAR(100)
);
GO

-- Brands
IF OBJECT_ID('bronze.production_brands', 'U') IS NOT NULL
    DROP TABLE bronze.production_brands;
GO

CREATE TABLE bronze.production_brands (
    brand_id        INT,
    brand_name      NVARCHAR(100)
);
GO

-- Products
IF OBJECT_ID('bronze.production_products', 'U') IS NOT NULL
    DROP TABLE bronze.production_products;
GO

CREATE TABLE bronze.production_products (
    product_id      INT,
    product_name    NVARCHAR(255),
    brand_id        INT,
    category_id     INT,
    model_year      INT,
    list_price      DECIMAL(10,2)
);
GO

-- Stocks
IF OBJECT_ID('bronze.production_stocks', 'U') IS NOT NULL
    DROP TABLE bronze.production_stocks;
GO

CREATE TABLE bronze.production_stocks (
    store_id        INT,
    product_id      INT,
    quantity        INT
);
GO

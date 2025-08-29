/*
==================================================
DDL Script : Create Silver Tables for Bike Store Dataset
==================================================
This script creates the Silver tables derived from Bronze tables.
All columns are cast to proper types, cleaned, and additional derived columns are added.
==================================================
*/

-- SALES TABLES ---------------------------------------------------

-- Customers
IF OBJECT_ID('silver.sales_customers', 'U') IS NOT NULL
    DROP TABLE silver.sales_customers;
GO

CREATE TABLE silver.sales_customers (
    customer_id     INT,
    first_name      NVARCHAR(100),
    last_name       NVARCHAR(100),
    phone           VARCHAR(10),
    email           NVARCHAR(255),
    street          NVARCHAR(255),
    city            NVARCHAR(100),
    state           CHAR(2),
    zip_code        CHAR(5),
    full_address    NVARCHAR(600)  -- derived column
);
GO

-- Staffs
IF OBJECT_ID('silver.sales_staffs', 'U') IS NOT NULL
    DROP TABLE silver.sales_staffs;
GO

CREATE TABLE silver.sales_staffs (
    staff_id        INT,
    first_name      NVARCHAR(100),
    last_name       NVARCHAR(100),
    email           NVARCHAR(255),
    phone           VARCHAR(10),
    active          INT,
    store_id        INT,
    manager_id      INT
);
GO

-- Stores
IF OBJECT_ID('silver.sales_stores', 'U') IS NOT NULL
    DROP TABLE silver.sales_stores;
GO

CREATE TABLE silver.sales_stores (
    store_id        INT,
    store_name      NVARCHAR(255),
    phone           VARCHAR(10),
    email           NVARCHAR(255),
    street          NVARCHAR(255),
    city            NVARCHAR(100),
    state           CHAR(2),
    zip_code        CHAR(5),
    full_address    NVARCHAR(600)  -- derived column
);
GO

-- Orders
IF OBJECT_ID('silver.sales_orders', 'U') IS NOT NULL
    DROP TABLE silver.sales_orders;
GO

CREATE TABLE silver.sales_orders (
    order_id           INT,
    customer_id        INT,
    order_status       INT,
    order_status_desc  NVARCHAR(20),   -- dervied : mapping
    store_id           INT,
    staff_id           INT,
    order_date         DATE,
    required_date      DATE,
    shipped_date       DATE,
    
    -- Derived columns
    ship_status        NVARCHAR(10),   -- 'Pending' / 'Completed'
    days_to_ship       INT,            -- shipped_date - order_date
    delay_vs_required  INT             -- shipped_date - required_date
);
GO

-- Order Items
IF OBJECT_ID('silver.sales_order_items', 'U') IS NOT NULL
    DROP TABLE silver.sales_order_items;
GO

CREATE TABLE silver.sales_order_items (
    order_id            INT,
    item_id             INT,
    product_id          INT,
    quantity            INT,
    list_price          DECIMAL(10,2),
    discount            DECIMAL(5,2),

    -- Derived columns
    discount_percentage DECIMAL(5,2),
    total_price         DECIMAL(12,2)
);
GO


-- PRODUCTION TABLES ---------------------------------------------

-- Categories
IF OBJECT_ID('silver.production_categories', 'U') IS NOT NULL
    DROP TABLE silver.production_categories;
GO

CREATE TABLE silver.production_categories (
    category_id     INT,
    category_name   NVARCHAR(100)
);
GO

-- Brands
IF OBJECT_ID('silver.production_brands', 'U') IS NOT NULL
    DROP TABLE silver.production_brands;
GO

CREATE TABLE silver.production_brands (
    brand_id        INT,
    brand_name      NVARCHAR(100)
);
GO

-- Products
IF OBJECT_ID('silver.production_products', 'U') IS NOT NULL
    DROP TABLE silver.production_products;
GO

CREATE TABLE silver.production_products (
    product_id      INT,
    product_name    NVARCHAR(255),
    brand_id        INT,
    category_id     INT,
    model_year      INT,
    list_price      DECIMAL(10,2)
);
GO

-- Stocks
IF OBJECT_ID('silver.production_stocks', 'U') IS NOT NULL
    DROP TABLE silver.production_stocks;
GO

CREATE TABLE silver.production_stocks (
    store_id        INT,
    product_id      INT,
    quantity        INT,
    out_of_stock    BIT  -- derived column: 1 if quantity = 0, else 0
);
GO


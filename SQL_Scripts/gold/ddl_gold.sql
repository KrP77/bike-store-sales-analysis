/*
==================================================
DDL Script : Create Gold Tables for Bike Store Dataset
==================================================
This script creates the Gold tables (Fact + Dimensions)
derived from Silver tables. It represents a dimensional 
model (Star Schema) for analytics & reporting.
==================================================
*/

-- DIMENSIONS ---------------------------------------------------

-- Customers
IF OBJECT_ID('gold.dim_customers', 'U') IS NOT NULL
    DROP TABLE gold.dim_customers;
GO

CREATE TABLE gold.dim_customers (
    customer_id     INT PRIMARY KEY,
    first_name      NVARCHAR(100),
    last_name       NVARCHAR(100),
    phone           VARCHAR(20),
    email           NVARCHAR(255),
    street          NVARCHAR(255),
    city            NVARCHAR(100),
    state           CHAR(2),
    zip_code        CHAR(10),
    full_address    NVARCHAR(500)
);
GO


-- Staffs
IF OBJECT_ID('gold.dim_staffs', 'U') IS NOT NULL
    DROP TABLE gold.dim_staffs;
GO

CREATE TABLE gold.dim_staffs (
    staff_id        INT PRIMARY KEY,
    first_name      NVARCHAR(100),
    last_name       NVARCHAR(100),
    phone           VARCHAR(20),
    email           NVARCHAR(255),
    active          INT,
    manager_id      INT,
    store_id        INT
);
GO


-- Stores
IF OBJECT_ID('gold.dim_stores', 'U') IS NOT NULL
    DROP TABLE gold.dim_stores;
GO

CREATE TABLE gold.dim_stores (
    store_id        INT PRIMARY KEY,
    store_name      NVARCHAR(255),
    phone           VARCHAR(20),
    email           NVARCHAR(255),
    street          NVARCHAR(255),
    city            NVARCHAR(100),
    state           CHAR(2),
    zip_code        CHAR(10),
    full_address    NVARCHAR(500)
);
GO


-- Products
IF OBJECT_ID('gold.dim_products', 'U') IS NOT NULL
    DROP TABLE gold.dim_products;
GO

CREATE TABLE gold.dim_products (
    product_id      INT PRIMARY KEY,
    category_id     INT,
    brand_id        INT,
    product_name    NVARCHAR(255),
    category_name   NVARCHAR(100),
    brand_name      NVARCHAR(100),
    model_year      INT,
    list_price      DECIMAL(10,2)
);
GO


-- FACTS -------------------------------------------------------

-- Sales Fact
IF OBJECT_ID('gold.fact_sales', 'U') IS NOT NULL
    DROP TABLE gold.fact_sales;
GO

CREATE TABLE gold.fact_sales (
    -- Primary Key
    order_id            INT,
    item_id             INT,

    -- Foreign Keys
    customer_id         INT,
    product_id          INT,
    staff_id            INT,
    store_id            INT,

    -- Data
    quantity            INT,
    list_price          DECIMAL(10,2),
    discount            DECIMAL(10,2),
    discount_percentage DECIMAL(5,2),
    total_price         DECIMAL(12,2),

    -- Order detail
    order_status        INT,
    order_status_desc   NVARCHAR(50),

    -- Dates and related metrics
    order_date          DATE,
    required_date       DATE,
    shipped_date        DATE,
    ship_status         NVARCHAR(20),
    days_to_ship        INT,
    delay_vs_required   INT,

    CONSTRAINT pk_fact_sales PRIMARY KEY (order_id, item_id)
);
GO


-- Stock Fact
IF OBJECT_ID('gold.fact_stocks', 'U') IS NOT NULL
    DROP TABLE gold.fact_stocks;
GO

CREATE TABLE gold.fact_stocks (
    store_id        INT,
    product_id      INT,
    quantity        INT,
    out_of_stock    BIT,
    CONSTRAINT pk_fact_stocks PRIMARY KEY (store_id, product_id)
);
GO

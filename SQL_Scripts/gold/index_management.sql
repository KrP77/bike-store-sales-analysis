/********************************************************************************************
    SCHEMA & INDEX MANAGEMENT SCRIPT FOR GOLD DATA MART
    Purpose: 
        1. Inspect schema design (columns, datatypes, nullability)
        2. Inspect existing indexes on all tables in gold schema
        3. Create foreign key supporting indexes on fact tables
********************************************************************************************/

---------------------------------------------------------------------------------------------
-- 1. CHECK COLUMN DETAILS FOR ALL TABLES IN GOLD SCHEMA
--    This query lists table name, column names, data types, max length, and nullability.
---------------------------------------------------------------------------------------------
SELECT 
    t.name AS TableName,
    c.name AS ColumnName,
    ty.name AS DataType,
    c.max_length AS MaxLength,
    c.is_nullable AS IsNullable
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.columns c ON t.object_id = c.object_id
INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
WHERE s.name = 'gold'
ORDER BY t.name, c.column_id;


---------------------------------------------------------------------------------------------
-- 2. CHECK EXISTING INDEXES ON ALL TABLES IN GOLD SCHEMA
--    This query shows all indexes, their type (CLUSTERED, NONCLUSTERED, COLUMNSTORE),
--    indexed columns, and included columns (if any).
---------------------------------------------------------------------------------------------
SELECT 
    t.name AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    c.name AS ColumnName,
    ic.is_included_column AS IsIncludedColumn
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
INNER JOIN sys.indexes i ON t.object_id = i.object_id
INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
INNER JOIN sys.columns c ON ic.object_id = c.object_id AND c.column_id = ic.column_id
WHERE s.name = 'gold'
ORDER BY t.name, i.name, ic.key_ordinal;


---------------------------------------------------------------------------------------------
-- 3. CREATE FOREIGN KEY SUPPORTING INDEXES (FOR JOIN PERFORMANCE)
--    Fact tables usually reference dimension tables by foreign keys.
--    Adding nonclustered indexes improves join & filter performance.
---------------------------------------------------------------------------------------------

-- Fact Sales foreign key indexes
DROP INDEX IF EXISTS IX_fact_sales_customer_id ON gold.fact_sales;
CREATE NONCLUSTERED INDEX IX_fact_sales_customer_id
ON gold.fact_sales(customer_id);

DROP INDEX IF EXISTS IX_fact_sales_product_id ON gold.fact_sales;
CREATE NONCLUSTERED INDEX IX_fact_sales_product_id
ON gold.fact_sales(product_id);

DROP INDEX IF EXISTS IX_fact_sales_staff_id ON gold.fact_sales;
CREATE NONCLUSTERED INDEX IX_fact_sales_staff_id
ON gold.fact_sales(staff_id);

DROP INDEX IF EXISTS IX_fact_sales_store_id ON gold.fact_sales;
CREATE NONCLUSTERED INDEX IX_fact_sales_store_id
ON gold.fact_sales(store_id);

-- Fact Stocks foreign key indexes
DROP INDEX IF EXISTS IX_fact_stocks_store_id ON gold.fact_stocks;
CREATE NONCLUSTERED INDEX IX_fact_stocks_store_id
ON gold.fact_stocks(store_id);

DROP INDEX IF EXISTS IX_fact_stocks_product_id ON gold.fact_stocks;
CREATE NONCLUSTERED INDEX IX_fact_stocks_product_id
ON gold.fact_stocks(product_id);



---------------------------------------------------------------------------------------------
-- NOTES:
-- * Run sections 1 & 2 before creating indexes to analyze schema and existing indexes.
-- * Section 3 adds missing supporting indexes on fact tables for better query performance.
-- * For training dataset (small row count), rowstore is fine. For production, 
--   consider columnstore indexes on fact tables for analytical queries.
---------------------------------------------------------------------------------------------

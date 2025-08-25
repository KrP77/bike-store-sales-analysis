/* 
===============================
Create Database and Schemas
===============================
We are using the medallion architecture for dwh project. Therefore we are creating bronze, silver and gold
layers. In this script we are creating a database named 'DataWarehouse' and making schemas bronze, silver and gold.
Note - this script will drop database 'DataWarehouse' if it exists already then create a new one as follows
*/
-- Create Database 'DataWarehouse'
USE master;
GO

-- Drop and create 'DataWarehouse' Database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO
  
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

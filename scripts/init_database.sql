/*  
==================================================
Create Database and Schemas
==================================================
Script Purpose:
	This script creates a new database named 'DataWarehouse' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally, the scripy sets up three schemas
	within the database: 'bronze','silver', and 'gold'.

Warning:
	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All the data in the database will be permanently deleted. Proceed with caution
	and ensure you have proper backups before running the script.
*/

Use master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

use DataWarehouse;
GO

-- Note: Schema is a folder or container that keeps a container organised. 
-- So, let's build a schema.
-- GO separate batches when working with multiple SQL statements.

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO


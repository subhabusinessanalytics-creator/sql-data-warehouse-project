/*
===================================================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===================================================================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to populate 
    the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into SIlver tables.
Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
=================================================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
		PRINT 'WELCOME! Let''s start the Silver Layer';
		PRINT '===============================================================================================';
		PRINT '------------------------------------------------------------------------------';
		PRINT 'Truncating the table: silver.crm_cust_info';
		SET @batch_start_time = GETDATE();
		SET @start_time = GETDATE();
		TRUNCATE table silver.crm_cust_info;
		PRINT 'TRUNCATE COMPLETE'
		PRINT '------------------------------------------------------------------------------'
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		Select 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 ELSE 'n/a'
		END cst_marital_status,		-- Normalize marital status values to readable format
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 ELSE 'n/a'
		END cst_gndr,				-- Normalize gender  values to readable format
		cst_create_date
		FROM (
			SELECT
			*,
			ROW_NUMBER() OVER (Partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)t WHERE flag_last = 1;
		SET @end_time = GETDATE();
		PRINT 'INSERTION COMPLETE in ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------------------------------------';
		PRINT 'Truncating the table: silver.crm_prd_info';
		SET @start_time = GETDATE();
		TRUNCATE table silver.crm_prd_info;
		PRINT 'TRUNCATE COMPLETE';
		PRINT '------------------------------------------------------------------------------';
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			   [prd_id]
			  ,[cat_id]
			  ,[prd_key]
			  ,[prd_nm]
			  ,[prd_cost]
			  ,[prd_line]
			  ,[prd_start_dt]
			  ,[prd_end_dt]
		)
		SELECT 
			   [prd_id]
			  ,REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS Category_ID						-- Extract Category ID
			  ,SUBSTRING(prd_key,7,len(prd_key)) AS Product_Key								-- Extract Product Key
			  ,[prd_nm]
			  ,COALESCE(prd_cost,0) as Product_Cost
			  ,CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
					WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
					WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
					WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
					ELSE 'n/a' 
				END AS Product_Line												-- Map Product Line codes to Descriptive Values
			  ,CAST([prd_start_dt] AS DATE) AS prd_start_dt
			  ,CAST(LEAD([prd_start_dt]) OVER (Partition By prd_key Order by prd_start_dt)-1 
			  AS DATE) AS prd_end_dt															-- Data Enrichment
		  FROM [DataWarehouse].[bronze].[crm_prd_info];
		SET @end_time = GETDATE();
		PRINT 'INSERTION COMPLETE in ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------------------------------------';
		PRINT 'Truncating the table: silver.crm_sales_details';
		SET @start_time = GETDATE();
		TRUNCATE table silver.crm_sales_details;
		PRINT 'TRUNCATE COMPLETE';
		PRINT '------------------------------------------------------------------------------';
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			   [sls_ord_num]
			  ,[sls_prd_key]
			  ,[sls_cust_id]
			  ,[sls_order_dt]
			  ,[sls_ship_dt]
			  ,[sls_due_dt]
			  ,[sls_sales]
			  ,[sls_quantity]
			  ,[sls_price]
		)
		SELECT 
			   [sls_ord_num]
			  ,[sls_prd_key]
			  ,[sls_cust_id]
			  ,CASE WHEN [sls_order_dt] = 0 or LEN([sls_order_dt]) != 8 THEN NULL
					ELSE CAST(CAST([sls_order_dt] AS VARCHAR) AS DATE)
				END AS [sls_order_dt]
			  ,CASE WHEN [sls_ship_dt] = 0 or LEN([sls_ship_dt]) != 8 THEN NULL
					ELSE CAST(CAST([sls_ship_dt] AS VARCHAR) AS DATE)
				END AS [sls_ship_dt]
			  ,CASE WHEN [sls_due_dt] = 0 or LEN([sls_due_dt]) != 8 THEN NULL
					ELSE CAST(CAST([sls_due_dt] AS VARCHAR) AS DATE)
				END AS [sls_due_dt]
			  ,CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
						THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
				END AS new_sls_sales
			  ,[sls_quantity]
			  ,CASE WHEN sls_price IS NULL or sls_price = 0 
						THEN ABS(sls_sales) / NULLIF(sls_quantity,0)
					WHEN sls_price < 0 THEN ABS(sls_price)
				ELSE sls_price
				END AS new_sls_price
		  FROM [DataWarehouse].[bronze].[crm_sales_details];
		SET @end_time = GETDATE();
		PRINT 'INSERTION COMPLETE in ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------------------------------------';
		PRINT 'Truncating the table: silver.erp_cust_az12';
		SET @start_time = GETDATE();
		TRUNCATE table silver.erp_cust_az12;
		PRINT 'TRUNCATE COMPLETE';
		PRINT '------------------------------------------------------------------------------';
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.[erp_cust_az12](
			cid,
			bdate,
			gen
			)
		SELECT 
			  CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
					ELSE cid
				END AS cid
			  ,CASE WHEN bdate > GETDATE() THEN NULL
					ELSE bdate
				END as bdate
			  ,CASE WHEN gen = 'M' THEN 'Male'
					 WHEN gen = 'F' THEN 'Female'
					 WHEN gen IS NULL THEN 'n/a'
					 WHEN gen = ' ' THEN 'n/a'
					 ELSE gen
				END AS gen
		  FROM [DataWarehouse].bronze.[erp_cust_az12];
		SET @end_time = GETDATE();
		PRINT 'INSERTION COMPLETE in ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------------------------------------';
		PRINT 'Truncating the table: silver.erp_loc_a101';
		SET @start_time = GETDATE();
		TRUNCATE table silver.erp_loc_a101;
		PRINT 'TRUNCATE COMPLETE';
		PRINT '------------------------------------------------------------------------------';
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		InSERT INTO silver.[erp_loc_a101](
			cid,
			cntry
		)
		SELECT
			REPLACE(cid,'-','') cid,
			CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
				 WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
				 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				 ELSE TRIM(cntry)
			END AS cntry
		from [bronze].[erp_loc_a101];
		SET @end_time = GETDATE();
		PRINT 'INSERTION COMPLETE in ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------------------------------------';
		PRINT 'Truncating the table: silver.erp_px_cat_g1v2';
		TRUNCATE table silver.erp_px_cat_g1v2;
		PRINT 'TRUNCATE COMPLETE';
		SET @start_time = GETDATE();
		PRINT '------------------------------------------------------------------------------';
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		SET @end_time = GETDATE();
		SET @batch_end_time = GETDATE();
		PRINT 'INSERTION COMPLETE in ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------------------------------------'
		PRINT 'THE Silver Layer is loaded in ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '===============================================================================================';
	END TRY
	BEGIN CATCH
		PRINT '===============================================================================================';
		PRINT 'THE ERROR MESSAGE IS' + ERROR_MESSAGE();
		PRINT 'THE ERROR NUMBER IS' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'THE ERROR STATE IS' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '===============================================================================================';
	END CATCH
END


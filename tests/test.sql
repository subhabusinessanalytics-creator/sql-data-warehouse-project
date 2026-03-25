-- Laer you can change the tests, as there is less time.
-- >> First Table in the Silver Layer -> Silver.crm_cust_info
-- -----------------------------------------------------------


-- Check for Nulls or Duplicates in Primary Key
-- Expectation : No Result

Select
cst_id,
count(*)
from bronze.crm_cust_info
group by cst_id
Having Count(*) > 1 OR cst_id IS NULL;

-- Data Cleaning (Null or Duplicates)

SELECT 
*
FROM (
SELECT
*,
ROW_NUMBER() OVER (Partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
)t where flag_last = 1;

-- Check for unwanted Spaces
-- Expectation: No Results

SELECT cst_firstname
FROM bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

-- Quality Check -> Check the consistency of values in low cardinality columns
-- DATA Standardization & Consistency

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

-- Some changes in the Silver Data Structure

ALTER TABLE silver.crm_cust_info
ALTER COLUMN cst_marital_status NVARCHAR(10)
ALTER TABLE silver.crm_cust_info
ALTER COLUMN cst_gndr NVARCHAR(10)

-- Data Transformation for removing NULL in Primary key & Data Trimming & Standardization
-- | ** Main Transformation ** | --
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
)t WHERE flag_last = 1		-- Select the most recent record per customer


-- Now Check again in SILVER LAYER
-- Check for Nulls or Duplicates in Primary Key
-- Expectation : No Result

Select
cst_id,
count(*)
from silver.crm_cust_info
group by cst_id
Having Count(*) > 1 OR cst_id IS NULL;

-- Data Cleaning (Null or Duplicates)

SELECT 
*
FROM (
SELECT
*,
ROW_NUMBER() OVER (Partition by cst_id order by cst_create_date desc) as flag_last
from silver.crm_cust_info
)t where flag_last = 1;

-- Check for unwanted Spaces
-- Expectation: No Results

SELECT cst_firstname
FROM silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

-- Quality Check -> Check the consistency of values in low cardinality columns
-- DATA Standardization & Consistency

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT * FROM silver.crm_cust_info;

-- ---------------------------------------------------------------------------------------------------
-- >> Second Table in the Silver Layer -> Silver.crm_prd_info
-- ---------------------------------------------------------------------------------------------------

-- First Check the Table from the Bronze Layer

SELECT * FROM bronze.crm_prd_info;
SELECT prd_id,Count(*) FROM bronze.crm_prd_info 
GROUP BY prd_id Having Count(*) IS NULL OR Count(*) > 1;			-- No Null Primary Key
SELECT * FROM bronze.crm_prd_info WHERE prd_nm != TRIM(prd_nm)		-- To Check for any spaces
SELECT prd_cost from bronze.crm_prd_info 
WHERE prd_cost <= 0 or prd_cost is NULL;							-- To check Null and Negative numbers
SELECT DISTINCT prd_line from bronze.crm_prd_info;					-- Standardize M, R, S, T in prd_line
SELECT * FROM bronze.crm_prd_info where prd_start_dt > prd_end_dt;	-- Here the Start date is greater than End Date
-- So in this case we can use the LEAD() or LAG() function for taking the next Start date as the present END Date.

-- Updating the DDL Command for silver.crm_prd_info

IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
DROP TABLE silver.crm_prd_info
CREATE TABLE silver.crm_prd_info(
	prd_id			int,
	cat_id			NVARCHAR(50),
	prd_key			NVARCHAR(50),
	prd_nm			NVARCHAR(50),
	prd_cost		INT,
	prd_line		NVARCHAR(50),
	prd_start_dt	DATE,
	prd_end_dt		DATE,
	dwh_create_date	DATETIME DEFAULT GETDATE()
	);


-- | ** Main Transformation ** | --

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

-- Quality Check of the Silver Table

SELECT * FROM silver.crm_prd_info;
SELECT prd_id,Count(*) FROM silver.crm_prd_info 
GROUP BY prd_id Having Count(*) IS NULL OR Count(*) > 1;			-- No Null Primary Key
SELECT * FROM silver.crm_prd_info WHERE prd_nm != TRIM(prd_nm)		-- To Check for any spaces
SELECT prd_cost from silver.crm_prd_info 
WHERE prd_cost < 0 or prd_cost is NULL;								-- To check Null and Negative numbers
SELECT DISTINCT prd_line from silver.crm_prd_info;					-- Standardize M, R, S, T in prd_line
SELECT * FROM silver.crm_prd_info where prd_start_dt > prd_end_dt;	-- Here the Start date is greater than End Date

-- Quality Check Done 

-- ---------------------------------------------------------------------------------------------------
-- >> Third Table in the Silver Layer -> Silver.crm_sales_details
-- ---------------------------------------------------------------------------------------------------

-- First Check the Table from the Bronze Layer

-- Check for Invalid Dates
SELECT NULLIF([sls_order_dt],0) from [bronze].[crm_sales_details] 
where [sls_order_dt] IS NULL or [sls_order_dt] <= 0 or 
LEN([sls_order_dt]) != 8 or [sls_order_dt] > 20500101
or [sls_order_dt] < 19000101

-- Check for Invalid Date orders
SELECT * from [bronze].[crm_sales_details] 
WHERE [sls_order_dt] > [sls_due_dt] or [sls_order_dt] > [sls_ship_dt]

-- Business Rule : Price * Quantity = Sales
SELECT DISTINCT
	sls_price AS Old_sls_price,
	sls_quantity,
	sls_sales AS Old_sls_sales,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS new_sls_sales,
	CASE WHEN sls_price IS NULL or sls_price = 0 
			THEN ABS(sls_sales) / NULLIF(sls_quantity,0)
		WHEN sls_price < 0 THEN ABS(sls_price)
	ELSE sls_price
	END AS new_sls_price
from [bronze].[crm_sales_details] 
WHERE sls_price * sls_quantity != sls_sales
or sls_price is NULL or sls_quantity is NULL or sls_sales is NULL 
or sls_price <=0 or sls_quantity <=0 or sls_sales <=0
order by sls_price,sls_quantity,sls_sales

/*	When these problems come with the cost and sales , first talk to the data engineer 
	IF they want to fix that from the source system, then leave it as it is
	But if they deny that, then follow a particular rule for these transformations.
	In this case, we will follow a rule too, to transform this data - 
	 - If Sales is Negative, zero, or null, then derive it using Quantity * Price.
	 - If price is zero, or null, then derive it using  Sales/Quantity .
	 - If price is negative, then convert it into positive.
*/


-- Updating the DDL Command for silver.crm_sales_details

IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
DROP TABLE silver.crm_sales_details
CREATE TABLE silver.crm_sales_details(
	 [sls_ord_num]		NVARCHAR(50)
	,[sls_prd_key]		NVARCHAR(50)
    ,[sls_cust_id]		INT
    ,[sls_order_dt]		DATE
    ,[sls_ship_dt]		DATE
    ,[sls_due_dt]		DATE
    ,[sls_sales]		INT
    ,[sls_quantity]		INT
    ,[sls_price]		INT
	,dwh_create_date	DATETIME DEFAULT GETDATE()
	);


-- | ** Main Transformation ** | --

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

  -- Transforming Complete 
 
 -- Last Check


-- Check for Invalid Date orders
SELECT * from silver.[crm_sales_details] 
WHERE [sls_order_dt] > [sls_due_dt] or [sls_order_dt] > [sls_ship_dt]

-- Business Rule : Price * Quantity = Sales
SELECT DISTINCT
	sls_price,
	sls_quantity,
	sls_sales
	from silver.[crm_sales_details] 
	WHERE sls_price * sls_quantity != sls_sales
	or sls_price is NULL or sls_quantity is NULL or sls_sales is NULL 
	or sls_price <=0 or sls_quantity <=0 or sls_sales <=0
	order by sls_price,sls_quantity,sls_sales;

-- Entire Clean Table
 SELECT * from silver.crm_sales_details;


-- ---------------------------------------------------------------------------------------------------
-- >> Fourth Table in the Silver Layer -> Silver.erp_cust_az12
-- ---------------------------------------------------------------------------------------------------

-- First Check the Table from the Bronze Layer

SELECT TOP (1000) [cid]
      ,[bdate]
      ,[gen]
  FROM [DataWarehouse].bronze.[erp_cust_az12]

 Select * from [silver].[crm_cust_info]

-- Check Distinct Gender

Select 
	Distinct gen,
	CASE WHEN gen = 'M' THEN 'Male'
		 WHEN gen = 'F' THEN 'Female'
		 WHEN gen IS NULL THEN 'n/a'
		 WHEN gen = ' ' THEN 'n/a'
		 ELSE gen
	END AS New_gen
from [DataWarehouse].bronze.[erp_cust_az12]

--Identify Out-of-Range Dates
Select Distinct bdate
from bronze.erp_cust_az12
where bdate < '1926-01-01' or bdate > GETDATE()

-- Primary key Check
SELECT cid,count(*) from bronze.[erp_cust_az12]
group by cid having count(*) > 1 or count(*) IS NULL;

-- |** Data Transformation **| --
INSERT INTO bronze.[erp_cust_az12](
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

-- Last Check

--Identify Out-of-Range Dates
Select Distinct bdate
from silver.erp_cust_az12
where bdate < '1926-01-01' or bdate > GETDATE()

-- Primary key Check
SELECT cid,count(*) from silver.[erp_cust_az12]
group by cid having count(*) > 1 or count(*) IS NULL;

-- ---------------------------------------------------------------------------------------------------
-- >> Fifth Table in the Silver Layer -> Silver.erp_loc_a101
-- ---------------------------------------------------------------------------------------------------

-- First Check the Table from the Bronze Layer

SELECT * from [bronze].[erp_loc_a101]

SELECT
	REPLACE(cid,'-','') cid,
	CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
		 WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry
from [bronze].[erp_loc_a101]

-- Data Standardisation & Consistency
Select DISTINCT cntry
from [bronze].[erp_loc_a101]

-- |** Data Transformation **| --

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

-- Data Cleaning Done

-- Last Check 

Select DISTINCT cntry
from Silver.[erp_loc_a101]

SELECT * from Silver.[erp_loc_a101]

-- ---------------------------------------------------------------------------------------------------
-- >> Sixth Table in the Silver Layer -> Silver.erp_px_cat_g1v2
-- ---------------------------------------------------------------------------------------------------

-- First Check the Table from the Bronze Layer

SELECT * from [bronze].[erp_px_cat_g1v2]
Select * from silver.[crm_prd_info]

Select * from bronze.erp_px_cat_g1v2 where TRIM(cat) != cat
Select * from bronze.erp_px_cat_g1v2 where TRIM(subcat) != subcat

-- Data Transformation & Load

INSERT INTO [silver].[erp_px_cat_g1v2](
id,
cat,
subcat,
maintenance)
SELECT * from bronze.[erp_px_cat_g1v2];

-- ---------------------------------------------------------------------------------------------------------

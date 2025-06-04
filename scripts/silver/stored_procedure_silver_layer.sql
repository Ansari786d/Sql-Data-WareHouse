/* 
===============================================================================================
Stored Procedure: Load Silver Layer (source > Bronze > Silver)
===============================================================================================
Scripts Purpose: 
		This stored procedure load the data into 'silver' schema(layer) from an 'bronze' layer 
		It perform the following actions.
		- Truncate the 'silver' tables before loading the data
		- Trnasform and standardize the values 
		- Cleans and transform the coluns with appropriate and meaningful values
		- Handles null and inconsistent values 
		- Enrich the data wherever needed
		
	Run this scripts to load the data into 'silver' Tables

Usage Example: EXEC silver.load_silver;
===============================================================================================
*/



USE DataWarehouse;
GO
CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN 

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

	BEGIN TRY 
		SET @batch_start_time = GETDATE();
		PRINT '=========================================================================';
		PRINT 'Loading Silver Layer';
		PRINT '=========================================================================' ;

		PRINT '--------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------------------------------------';


		PRINT '>> Truncating the Table : silver.crm_cust_info ';

		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting the Data into silver.crm_cust_info ';

		INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)

		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		CASE 
			 WHEN UPPER(cst_marital_status) = 'M' THEN 'MARRIED'
			 WHEN UPPER(cst_marital_status) = 'S' THEN 'SINGLE'
			 ELSE 'n/a'-- Normalize marital status to more readable format 
		END AS cst_marital_status,
		CASE
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
			 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
			 ELSE 'n/a' -- Normalize gender values to more readable format 
		END AS cst_gndr,
		cst_create_date
		FROM 
		(SELECT *,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as last_flag
		FROM bronze.crm_cust_info ) t 
		WHERE last_flag = 1  -- Select the most recent record per customer 

		----------------------------------------------------------------------------------------------------
		-- Cleaning & Trasforming crm_prod_info table 
		----------------------------------------------------------------------------------------------------

		PRINT '>> Truncating the Table : silver.crm_prd_info ';

		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting the Data into  silver.crm_prd_info ';

		INSERT INTO silver.crm_prd_info(
			prd_id, 
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)

		SELECT 
		prd_id, 
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id, -- Extract category ID 
		SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,      -- Extract product key 
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE 
			WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'MOUNTAIN'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'ROAD'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line, -- Map product line codes to descriptive value 
		CAST (prd_start_dt AS DATE) AS prd_start_dt,
		CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt -- calculate end date one day before the next start date 
		FROM bronze.crm_prd_info




		--------------------------------------------------------------------------------------------------------------------------------------
		-- Cleaning crm_sales_details table 
		--------------------------------------------------------------------------------------------------------------------------------------


		PRINT '>> Truncating the Table : silver.crm_sales_details ';

		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting the Data into  silver.crm_sales_details ';

		INSERT INTO silver.crm_sales_details(
		 sls_ord_num,		
		 sls_prd_key,		
		 sls_cust_id,		
		 sls_order_dt,		
		 sls_ship_dt,		
		 sls_due_dt,			
		 sls_sales,			
		 sls_quantity,		
		 sls_price

		)

		SELECT 
		 sls_ord_num,
		 sls_prd_key,
		 sls_cust_id,
		 CASE 
			WHEN sls_order_dt < 0 OR LEN(sls_order_dt) !=8 THEN NULL 
			ELSE CAST( CAST(sls_order_dt AS VARCHAR) AS DATE) 
		 END AS sls_order_dt,
		 CASE 
			WHEN sls_ship_dt < 0 OR LEN(sls_ship_dt) !=8 THEN NULL 
			ELSE CAST( CAST(sls_ship_dt AS VARCHAR) AS DATE) 
		 END AS sls_ship_dt,
		 CASE 
			WHEN sls_due_dt < 0 OR LEN(sls_due_dt) !=8 THEN NULL 
			ELSE CAST( CAST(sls_due_dt AS VARCHAR) AS DATE) 
		 END AS sls_due_dt,	

		 CASE
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales !=  sls_quantity * ABS(sls_price)
				THEN  sls_quantity * ABS(sls_price)
			ELSE sls_sales
		 END AS sls_sales,
		 sls_quantity,

		 CASE
			WHEN sls_price IS NULL OR sls_price <= 0 
				THEN  sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
		 END AS sls_price
		FROM bronze.crm_sales_details

		--------------------------------------------------------------------------------------------------------------------------------------
		-- Cleaning erp_cust table 
		--------------------------------------------------------------------------------------------------------------------------------------
		PRINT '--------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------------------------------------------';
		PRINT '>> Truncating the Table : silver.erp_cust_az12 ';

		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting the Data into  silver.erp_cust_az12 ';

		INSERT INTO silver.erp_cust_az12(
		cid	,					
		bdate,			
		gen				

		)
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' Prefix if present 
			 ELSE cid 
		END cid,
		CASE WHEN bdate > GETDATE() THEN NULL 
			 ELSE bdate
		END as bdate, -- We set future dates to null 
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			 ELSE 'n/a'
		END AS gen

		FROM bronze.erp_cust_az12



		--------------------------------------------------------------------------------------------------------------------------------------
		-- Cleaning erp_cust table 
		--------------------------------------------------------------------------------------------------------------------------------------
		PRINT '>> Truncating the Table : silver.erp_loc_a101 ';

		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting the Data into  silver.erp_loc_a101 ';


		INSERT INTO silver.erp_loc_a101(

		cid, 
		cntry
		)
		SELECT 
		REPLACE(cid,'-','') as  cid,
 
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN UPPER(TRIM(cntry)) IN ('US', 'USA' , 'UNITED STATES') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END AS cntry

		FROM bronze.erp_loc_a101

	



		--------------------------------------------------------------------------------------------------------------------------------------
		-- Cleaning erp_cust table 
		--------------------------------------------------------------------------------------------------------------------------------------

		PRINT '>> Truncating the Table : silver.erp_px_cat_g1v2 ';

		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting the Data into  silver.erp_px_cat_g1v2 ';

		INSERT INTO silver.erp_px_cat_g1v2(

		id, 
		cat,
		subcat,
		maintenance

		)

		SELECT 
		id, 
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2

		SET @batch_end_time = GETDATE();
		PRINT '=========================================================================';
		PRINT 'Loading Silver Layer is completed ';
		PRINT ' Total Load Duration: ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT ' --------------------------------------- ';
	END TRY 
	
	BEGIN CATCH 
	PRINT '=========================================================================';
	PRINT 'ERROR OCCURRED DURING Loading Silver Layer';
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Number' + CAST (ERROR_NUMBER() As VARCHAR);
	PRINT 'Error State' + CAST (ERROR_STATE() As VARCHAR);
	PRINT '=========================================================================' ;
	END CATCH 

END 


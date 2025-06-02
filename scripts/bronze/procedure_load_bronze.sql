/* 
===============================================================================================
Stored Procedure: Load Bronze Layer (source > Bronze)
===============================================================================================
Scripts Purpose: 
		This stored procedure load the data into 'bronze' schema(layer) from an external csv files 
		It perform the following actions.
		- Truncate the'bronze' tables before loading the data
		- Use BULK INSERT command to load the data from csv files
	Run this scripts to load the data into 'bronze' Tables

Usage Example: EXEC bronze.load_bronze;
===============================================================================================
*/
USE DataWarehouse;
GO
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

	BEGIN TRY 
		SET @batch_start_time = GETDATE();
		PRINT '=========================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=========================================================================' ;

		PRINT '--------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------------------------------------';
		

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : crm_cust_info ';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data Into : crm_cust_info ';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\ansar\Downloads\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------------- '
		


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : crm_prd_info ';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data Into : crm_prd_info ';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\ansar\Downloads\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------------- '


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : crm_sales_details ';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into : crm_sales_details ';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\ansar\Downloads\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------------- '



		PRINT '--------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '--------------------------------------------------------------------------';



		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : erp_loc_a101 ';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into : erp_loc_a101 ';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\ansar\Downloads\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------------- '



		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : erp_cust_az12 ';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into : erp_cust_az12 ';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\ansar\Downloads\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------------- '



		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : erp_px_cat_g1v2 ';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into : erp_px_cat_g1v2 ';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\ansar\Downloads\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> --------------------------------------- '

		SET @batch_end_time = GETDATE();
		PRINT '=========================================================================';
		PRINT 'Loading Bronze Layer is completed ';
		PRINT ' Total Load Duration: ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT ' --------------------------------------- ';

	END TRY
	BEGIN CATCH 
	PRINT '=========================================================================';
	PRINT 'ERROR OCCURRED DURING Loading Bronze Layer';
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Number' + CAST (ERROR_NUMBER() As VARCHAR);
	PRINT 'Error State' + CAST (ERROR_STATE() As VARCHAR);
	PRINT '=========================================================================' ;
	END CATCH 
END



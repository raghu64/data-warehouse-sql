/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
This Stored Procedure loads data into 'bronze' schema from external CSV files.
This will first truncate the table and then loads the data using 'BULK INSERT'

Parameters:
	file_path: the local project path

Example: EXEC bronze.load_bronze @file_path = 'D:\sql\project
===============================================================================
*/
USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze @file_path NVARCHAR(500) AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	DECLARE @crm_cust_info_full_path NVARCHAR(500), @crm_prd_info_full_path NVARCHAR(500), @crm_sales_details_full_path NVARCHAR(500);
	DECLARE @erp_cust_az12_full_path NVARCHAR(500), @erp_loc_a101_full_path NVARCHAR(500), @erp_px_cat_g1v2_full_path NVARCHAR(500); 
	DECLARE @SQL NVARCHAR(MAX);

	BEGIN TRY

		SET @batch_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		SET @crm_cust_info_full_path = @file_path + '\datasets\source_crm\cust_info.csv'
		SET @SQL = '
			BULK INSERT	bronze.crm_cust_info
			FROM ''' + @crm_cust_info_full_path + '''
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				TABLOCK
			);
		';
		EXEC sp_executesql @SQL;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		SET @crm_prd_info_full_path = @file_path + '\datasets\source_crm\prd_info.csv'
		SET @SQL = '
			BULK INSERT	bronze.crm_prd_info
			FROM ''' + @crm_prd_info_full_path + '''
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				TABLOCK
			);
		';
		EXEC sp_executesql @SQL;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		SET @crm_sales_details_full_path = @file_path + '\datasets\source_crm\sales_details.csv'
		SET @SQL = '
			BULK INSERT	bronze.crm_sales_details
			FROM ''' + @crm_sales_details_full_path + '''
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				TABLOCK
			);
		';
		EXEC sp_executesql @SQL;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		SET @erp_cust_az12_full_path = @file_path + '\datasets\source_erp\CUST_AZ12.csv'
		SET @SQL = '
			BULK INSERT	bronze.erp_cust_az12
			FROM ''' + @erp_cust_az12_full_path + '''
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				TABLOCK
			);
		';
		EXEC sp_executesql @SQL;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		SET @erp_loc_a101_full_path = @file_path + '\datasets\source_erp\LOC_A101.csv'
		SET @SQL = '
			BULK INSERT	bronze.erp_loc_a101
			FROM ''' + @erp_loc_a101_full_path + '''
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				TABLOCK
			);
		';
		EXEC sp_executesql @SQL;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		SET @erp_px_cat_g1v2_full_path = @file_path + '\datasets\source_erp\PX_CAT_G1V2.csv'
		SET @SQL = '
			BULK INSERT	bronze.erp_px_cat_g1v2
			FROM ''' + @erp_px_cat_g1v2_full_path + '''
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				TABLOCK
			);
		';
		EXEC sp_executesql @SQL;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
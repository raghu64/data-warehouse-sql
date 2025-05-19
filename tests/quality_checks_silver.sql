/*
crm_cust_info
*/
-- check for duplicates and nulls in the primary key

select cst_id, count(*) from bronze.crm_cust_info
group by cst_id
having count(*) > 1

select * from  bronze.crm_cust_info where cst_id is null

select * from (
select *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY  cst_create_date DESC) as rank_row
from bronze.crm_cust_info 
-- where cst_id in (29449,29473,29433,29483,29466)
) t where rank_row = 1


-- check for unwanted spaces
select cst_firstname from bronze.crm_cust_info where cst_firstname != trim(cst_firstname);
select cst_lastname from bronze.crm_cust_info where cst_lastname != trim(cst_lastname);
select cst_marital_status from bronze.crm_cust_info where cst_marital_status != trim(cst_marital_status);
select cst_gndr from bronze.crm_cust_info where cst_gndr != trim(cst_gndr);


select 
cst_id,
cst_key,
trim(cst_firstname),
trim(cst_lastname),
cst_marital_status.
cst_gndr,
cst_create_date
from (
select *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY  cst_create_date DESC) AS rank_row
FROM bronze.crm_cust_info 
) t WHERE rank_row = 1

-- check for standerdization and consistency
select distinct cst_gndr from bronze.crm_cust_info;
select distinct cst_marital_status from bronze.crm_cust_info;

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	ELSE 'n/a'
END cst_marital_status,  -- Normalize marital status value to readle format
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'n/a'
END cst_gndr, -- Normalize gender value to readle format
cst_create_date
FROM (
	SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY  cst_create_date DESC) AS rank_row
	FROM bronze.crm_cust_info 
) t 
WHERE rank_row = 1 -- select the most recent record per customer



/*
crm_prd_info
*/

-- check for duplicates and nulls in the primary key

select prd_id, count(*) from bronze.crm_prd_info
group by prd_id
having count(*) > 1


-- check for unwanted spaces
select prd_nm from bronze.crm_prd_info where prd_nm != trim(prd_nm);

-- check for null or negative numbers
select prd_cost from bronze.crm_prd_info where prd_cost < 0 or prd_cost is null;


-- check for standerdization and consistency
select distinct prd_line from bronze.crm_prd_info;

-- check for invalid date orders
select * from bronze.crm_prd_info where prd_end_dt <= prd_start_dt;

-- to access information of other record from the current record, we can use windows functions
-- LEAD() - access values from the next row within a window
-- LAG() - access values from the previous row with in a window
select 
	prd_id, 
	prd_key, 
	prd_nm, 
	prd_cost, 
	prd_line, 
	prd_start_dt, 
	prd_end_dt,
	LEAD(prd_start_dt) OVER (partition by prd_key order by prd_start_dt asc)-1 as prd_end_dt_tmp
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');


/*
crm_sales_details
*/

-- check for duplicates and nulls in the primary key

select sls_ord_num, count(*) from bronze.crm_sales_details
group by sls_ord_num
having count(*) > 1

-- check for invalid dates

select 
sls_order_dt 
from bronze.crm_sales_details
where sls_order_dt <= 0 
or LEN(sls_order_dt) != 8 
or sls_order_dt > 20260101 
or sls_order_dt < 19990101


select 
sls_ship_dt 
from bronze.crm_sales_details
where sls_ship_dt <= 0 
or LEN(sls_ship_dt) != 8 
or sls_ship_dt > 20260101 
or sls_ship_dt < 19990101


select 
sls_due_dt 
from bronze.crm_sales_details
where sls_due_dt <= 0 
or LEN(sls_due_dt) != 8 
or sls_due_dt > 20260101 
or sls_due_dt < 19990101


-- check fro invalid date orders

select * from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- check data consistency between sales, quantity and price
-- sales = quantity * price
-- values must not be null, zero, or negative


/*
transformation rules:
if sales is -ve, 0, or null - derivce it usign quantity and price
if price is 0, or null - calculate  it from sales and quantity
if price is -ve - convert it to positive
*/
select 
sls_sales as old_sls_sales, sls_quantity, sls_price as old_sls_price
, CASE
		WHEN sls_sales <= 0 OR sls_sales IS NULL  OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END sls_sales

	, CASE
		WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
		-- WHEN sls_price < 0 THEN ABS(sls_price)
		ELSE sls_price
	END sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0  or sls_price <= 0 
order by sls_sales, sls_quantity, sls_price



/*
erp_cust_az12
*/

-- identify out of range dates
select 
	bdate 
from bronze.erp_cust_az12
where bdate > GETDATE() or bdate < '1920-01-01'

-- check data consistency
select distinct gen from bronze.erp_cust_az12

/*
erp_loc_a101*/
-- data standarization and consistency
select distinct cntry from bronze.erp_loc_a101 order by cntry


/*
erp_px_cat_g1v2
*/

select distinct maintenance from bronze.erp_px_cat_g1v2
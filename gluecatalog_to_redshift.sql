CREATE OR REPLACE PROCEDURE dss_fhd_stage.gluecatalog_to_redshift(redshift_schema varchar,redshift_table varchar,s3_path varchar)
	LANGUAGE plpgsql
AS $$
	 
DECLARE 
	SQLscript VARCHAR(1000) := '';
	SQL_create_table VARCHAR(1000) := '';
	SQL_truncate_table VARCHAR(100) := '';
	tablecount INT := 0;
	schema_table varchar(201) := redshift_schema || '.' || redshift_table;
	SQL_table_swap_step1 varchar(100) := '';
	SQL_table_swap_step2 varchar(100) := '';
	SQL_table_swap_step3 varchar(100) := '';
	currentuser varchar(100) := (select current_user);
BEGIN
	RAISE NOTICE '---------------';
	RAISE NOTICE 'redshift_schema: %',redshift_schema;
	RAISE NOTICE 'redshift_table: %',redshift_table;
	RAISE NOTICE 's3_path: %',s3_path;
	RAISE NOTICE 'SQLscript Before: %',SQLscript;	                                              
	RAISE NOTICE 'tablecount Before: %',tablecount;	                                              
	
	SELECT
		INTO tablecount nvl(COUNT(*), 0) 
	FROM
		information_schema.tables 
	WHERE
		table_type = 'BASE TABLE' 
		AND table_schema = redshift_schema
		AND table_name = redshift_table || '_swap';
    
    RAISE NOTICE 'tablecount after: %',tablecount;	                                                                                         


   
	IF tablecount = 0 THEN
    	BEGIN
        	--Create "swap" table if it doesn't exist.
	    	SQL_create_table := 'create table ' || schema_table || '_swap as select * from ' || schema_table || ' where 1=0';
	    	RAISE NOTICE 'Create "swap" table: %',SQL_create_table;	
        	EXECUTE (SQL_create_table);
      	END;
      ELSE
    	BEGIN	    	
	        --If the "swap" table exists then truncate it for the new load
	    	SQL_truncate_table := 'truncate table ' || schema_table || '_swap';
	    	RAISE NOTICE 'Swap table exists, truncate for new load: %',SQL_truncate_table;
	        EXECUTE (SQL_truncate_table);
		END;       
    END IF;
  
    
    --
    --  Copy data over from s3 to "swap" table
    --
	SQLscript := 
		'copy ' || schema_table || '_swap' || 
		' from ''' ||s3_path || ''''|| 
		' iam_role ''arn:aws:iam::443532969728:role/adhoc-datawarehouse-glue''' ||
		' format as parquet ' ||
		' COMPUPDATE OFF STATUPDATE OFF';
	RAISE NOTICE  'Copy table from s3: %',SQLscript;
	EXECUTE SQLscript;

	-- Make table owner the user running this script
	--  This allows the table rename to happen
	--select tablename, tableowner From pg_tables WHERE tablename = 'date_dim'
	--   	SQLscript := 'alter table ' || schema_table || ' owner to ' || currentuser;
	--    RAISE NOTICE 'table Alter: %',SQLscript;	
	--	EXECUTE SQLscript;
	   


	--
	--   Perform a table swap of the original table to the "swap" table just pulled in
	--
	SQL_table_swap_step1 := 'ALTER TABLE ' || schema_table || ' RENAME TO ' || redshift_table || '_newswap';
	RAISE NOTICE  'SQL_table_swap_step1: %',SQL_table_swap_step1;
	EXECUTE (SQL_table_swap_step1);
	--
	SQL_table_swap_step2 := 'ALTER TABLE ' || schema_table || '_swap RENAME TO ' || redshift_table;
	RAISE NOTICE  'SQL_table_swap_step2: %',SQL_table_swap_step2;
	EXECUTE (SQL_table_swap_step2);
	--
	SQL_table_swap_step3 := 'ALTER TABLE ' || schema_table || '_newswap RENAME TO ' || redshift_table || '_swap'; 
	RAISE NOTICE  'SQL_table_swap_step3: %',SQL_table_swap_step3;
	EXECUTE (SQL_table_swap_step3 );

    -- Drop swap table
    EXECUTE ('DROP TABLE ' || redshift_table || '_swap');                        	
END;

$$
;

CREATE OR REPLACE PROCEDURE "DIM_DATE_LOAD"("JOB_ID" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
    var sql_command_load = 
     `MERGE INTO STORE_DB.DATA_MART.DIM_DATE TRG 
	USING 
	(
	WITH CTE_DATE AS (
		SELECT DATEADD(DAY, SEQ4(), ''2012-01-01'') AS MY_DATE
		FROM TABLE(GENERATOR(ROWCOUNT=>3500))
		)
		SELECT
		substr(regexp_replace(MY_DATE,''[^0-9]''),0,8) AS DATE_ID 
		,TO_DATE(MY_DATE) as date
		,YEAR(MY_DATE) as year
		,MONTH(MY_DATE) as month
		,MONTHNAME(MY_DATE) as monthname
		,DAY(MY_DATE) as day
		,NVL(TYPE,''-'') AS TYPE
		,NVL(LOCALE,''-'') AS LOCALE
		,NVL(LOCALE_NAME,''-'') AS LOCALE_NAME
		,NVL(DESCRIPTION,''-'') AS DESCRIPTION
		,NVL(TRANSFERRED,''-'') AS TRANSFERRED
		FROM CTE_DATE D
		LEFT JOIN STORE_DB.STAGE.TMP_HOLIDAYS_EVENTS HE ON HE.DATE = D.MY_DATE  
		MINUS 
		SELECT 
		DATE_ID
		, DATE
		, YEAR
		, MONTH
		, MONTHNAME
		, DAY
		, TYPE
		, LOCALE
		, LOCALE_NAME
		, DESCRIPTION
		, TRANSFERRED
		FROM 
		STORE_DB.DATA_MART.DIM_DATE
		
	) SRC
	ON (TRG.DATE_ID = SRC.DATE_ID)
	WHEN MATCHED THEN 
	UPDATE SET 
		TRG.DATE =  SRC.DATE
		,TRG.YEAR =  SRC.YEAR
		,TRG.MONTH =  SRC.MONTH
		,TRG.MONTHNAME =  SRC.MONTHNAME
		,TRG.DAY =  SRC.DAY
		,TRG.TYPE =  SRC.TYPE
		,TRG.LOCALE = SRC.LOCALE
		,TRG.LOCALE_NAME =  SRC.LOCALE_NAME
		,TRG.DESCRIPTION = SRC.DESCRIPTION
		,TRG.TRANSFERRED = SRC.TRANSFERRED
		,TRG.T_TIMESTAMP = CURRENT_TIMESTAMP
		,TRG.T_JOB_ID = ''`+ JOB_ID +`''
	WHEN NOT MATCHED THEN 
	INSERT (DATE_ID, DATE, YEAR, MONTH, MONTHNAME, DAY, TYPE, LOCALE, LOCALE_NAME, DESCRIPTION, TRANSFERRED, T_JOB_ID)
	VALUES (SRC.DATE_ID, SRC.DATE, SRC.YEAR, SRC.MONTH, SRC.MONTHNAME, SRC.DAY, SRC.TYPE, SRC.LOCALE, SRC.LOCALE_NAME, SRC.DESCRIPTION, SRC.TRANSFERRED, ''`+ JOB_ID +`'');` 

	var sql_command_truncate_tmp = 
     `TRUNCATE TABLE STORE_DB.STAGE.TMP_HOLIDAYS_EVENTS`;
    try {
        snowflake.execute (
            {sqlText: sql_command_load}
            );
		snowflake.execute (
            {sqlText: sql_command_truncate_tmp}
            );
        return "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    ';
CREATE OR REPLACE PROCEDURE "DIM_ITEMS_LOAD"("JOB_ID" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
    var sql_command_load = 
     `MERGE INTO STORE_DB.DATA_MART.DIM_ITEMS TRG 
	USING 
	(SELECT ITEM_NBR, NVL(FAMILY,''-'') AS FAMILY, NVL(CLASS,''-'') AS CLASS, NVL(PERISHABLE,''-'') AS PERISHABLE FROM STORE_DB.STAGE.TMP_ITEMS 
	 MINUS
	 SELECT ITEM_NBR, NVL(FAMILY,''-'') AS FAMILY, NVL(CLASS,''-'') AS CLASS, NVL(PERISHABLE,''-'') AS PERISHABLE FROM STORE_DB.DATA_MART.DIM_ITEMS) AS SRC 
	ON TRG.ITEM_NBR = SRC.ITEM_NBR
	WHEN MATCHED THEN 
	UPDATE SET TRG.FAMILY = SRC.FAMILY
        , TRG.CLASS = SRC.CLASS
        , TRG.PERISHABLE = SRC.PERISHABLE
        , TRG.T_TIMESTAMP = CURRENT_TIMESTAMP
        , TRG.T_JOB_ID = ''`+JOB_ID+`'' 
	WHEN NOT MATCHED THEN 
	INSERT ( ITEM_NBR, FAMILY, CLASS, PERISHABLE,	T_JOB_ID)  
	VALUES ( SRC.ITEM_NBR, SRC.FAMILY, SRC.CLASS, SRC.PERISHABLE,	''`+JOB_ID+`'');`
	var sql_command_truncate_tmp = 
     `TRUNCATE TABLE STORE_DB.STAGE.TMP_ITEMS`;
    try {
        snowflake.execute (
            {sqlText: sql_command_load}
            );
		snowflake.execute (
            {sqlText: sql_command_truncate_tmp}
            );
        return "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    ';
CREATE OR REPLACE PROCEDURE "DIM_STORES_LOAD"("JOB_ID" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
    var sql_command_load = 
     `MERGE INTO STORE_DB.DATA_MART.DIM_STORES TRG 
	USING 
	(SELECT STORE_NBR, NVL(CITY,''-'') AS CITY, NVL(STATE,''-'') AS STATE, NVL(TYPE,''-'') AS STATE, NVL(CLUSTER,''-'') AS CLUSTER FROM STORE_DB.STAGE.TMP_STORES 
	MINUS
	 SELECT STORE_NBR, NVL(CITY,''-'') AS CITY, NVL(STATE,''-'') AS STATE, NVL(TYPE,''-'') AS STATE, NVL(CLUSTER,''-'') AS CLUSTER FROM STORE_DB.DATA_MART.DIM_STORES) AS SRC 
	ON TRG.STORE_NBR = SRC.STORE_NBR
	WHEN MATCHED THEN 
	UPDATE SET TRG.CITY = SRC.CITY
        , TRG.STATE = SRC.STATE
        , TRG.TYPE = SRC.TYPE
        , TRG.CLUSTER = SRC.CLUSTER
        , TRG.T_TIMESTAMP = CURRENT_TIMESTAMP
        , TRG.T_JOB_ID = ''`+JOB_ID+`'' 
	WHEN NOT MATCHED THEN 
	INSERT ( STORE_NBR, CITY, STATE, TYPE, CLUSTER,	T_JOB_ID)  
	VALUES ( SRC.STORE_NBR, SRC.CITY, SRC.STATE, SRC.TYPE, SRC.CLUSTER,	''`+JOB_ID+`'');`
	var sql_command_truncate_tmp = 
     `TRUNCATE TABLE STORE_DB.STAGE.TMP_STORES`;
    try {
        snowflake.execute (
            {sqlText: sql_command_load}
            );
		snowflake.execute (
            {sqlText: sql_command_truncate_tmp}
            );
        return "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    ';
CREATE OR REPLACE PROCEDURE "FACT_EVENTS_LOAD"("JOB_ID" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
    var sql_command_load = 
     `MERGE INTO STORE_DB.DATA_MART.FACT_EVENTS TRG
	USING 
	(
	SELECT 
	EV.ID AS SRC_ID 
	, to_number(regexp_replace(EV.DATE,''[^0-9]'')) AS DATE_ID
	, nvl(ST.STORES_ID,-1) as  STORES_ID
	, nvl(IT.ITEMS_ID,-1) as ITEMS_ID
	, EV.UNIT_SALES,
	, nvl(EV.ONPROMOTION,''-'') as ONPROMOTION
	FROM 
	STORE_DB.STAGE.TMP_ALL_EVENTS EV
	LEFT JOIN STORE_DB.DATA_MART.DIM_STORES ST ON EV.STORE_NBR = ST.STORE_NBR 
	LEFT JOIN STORE_DB.DATA_MART.DIM_ITEMS IT ON EV.ITEM_NBR = IT.ITEM_NBR
	MINUS 
	SELECT 
	SRC_ID
	, DATE_ID
	, STORES_ID
	, ITEMS_ID
	, UNIT_SALES
	, nvl(ONPROMOTION,''-'') as ONPROMOTION
	FROM STORE_DB.DATA_MART.FACT_EVENTS
	) SRC
	ON (TRG.DATE_ID = SRC.DATE_ID)
	WHEN MATCHED THEN 
		UPDATE SET 
			TRG.SRC_ID = SRC.SRC_ID
		, TRG.DATE_ID = SRC.DATE_ID 
		, TRG.STORES_ID = SRC.STORES_ID 
		, TRG.ITEMS_ID = SRC.ITEMS_ID
		, TRG.UNIT_SALES = SRC.UNIT_SALES
		, TRG.ONPROMOTION = SRC.ONPROMOTION
		, TRG.T_TIMESTAMP = CURRENT_TIMESTAMP
		, TRG.T_JOB_ID = ''`+ JOB_ID +`''
	WHEN NOT MATCHED THEN 
	INSERT (SRC_ID, DATE_ID, STORES_ID, ITEMS_ID, UNIT_SALES, ONPROMOTION, T_JOB_ID)
	VALUES (SRC.SRC_ID, SRC.DATE_ID, SRC.STORES_ID, SRC.ITEMS_ID, SRC.UNIT_SALES, SRC.ONPROMOTION, ''`+ JOB_ID +`'');`
	var sql_command_truncate_tmp = 
     `TRUNCATE TABLE STORE_DB.STAGE.TMP_ALL_EVENTS`;
    try {
        snowflake.execute (
            {sqlText: sql_command_load}
            );
		snowflake.execute (
            {sqlText: sql_command_truncate_tmp}
            );
        return "Succeeded.";   
        }
    catch (err)  {
        return "Failed: " + err;   
        }
    ';
CREATE OR REPLACE PROCEDURE "FACT_TRANSACTIONS_LOAD"("JOB_ID" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
    var sql_command_load = 
     `MERGE INTO STORE_DB.DATA_MART.FACT_TRANSACTIONS TRG 
	USING 
	(
	with CTE_TRANSACTIONS (DATE, STORE_NBR, TRANSACTIONS) 
	AS
	(
	SELECT 
	DATE
	, STORE_NBR
	, SUM(TRANSACTIONS) AS TRANSACTIONS
	FROM 
	STORE_DB.STAGE.TMP_TRANSACTIONS 
	GROUP BY 
	DATE, STORE_NBR
	)
	select 
	to_number(regexp_replace(TR.DATE,''[^0-9]'')) AS DATE_ID
	, COALESCE(ST.STORES_ID, -1) AS STORES_ID
	, TR.TRANSACTIONS
	from 
	CTE_TRANSACTIONS TR
	left join 
	STORE_DB.DATA_MART.DIM_STORES ST
	on  
	TR.STORE_NBR = ST.STORE_NBR 
	MINUS 
	SELECT 
		DATE_ID
	, STORES_ID
	, TRANSACTIONS
	FROM 
	STORE_DB.DATA_MART.FACT_TRANSACTIONS  
	) SRC
	ON (TRG.DATE_ID = SRC.DATE_ID)
	WHEN MATCHED THEN 
	UPDATE SET 
		TRG.STORES_ID = SRC.STORES_ID 
		, TRG.TRANSACTIONS = SRC.TRANSACTIONS
        , T_TIMESTAMP = CURRENT_TIMESTAMP
		, TRG.T_JOB_ID = ''`+ JOB_ID +`''
	WHEN NOT MATCHED THEN 
	INSERT (DATE_ID, STORES_ID, TRANSACTIONS, T_JOB_ID)
	VALUES (SRC.DATE_ID, SRC.STORES_ID, SRC.TRANSACTIONS, ''`+ JOB_ID +`'') 
	;`
	var sql_command_truncate_tmp = 
     `TRUNCATE TABLE STORE_DB.STAGE.TMP_TRANSACTIONS`;
    try {
        snowflake.execute (
            {sqlText: sql_command_load}
            );
		snowflake.execute (
            {sqlText: sql_command_truncate_tmp}
            );
        return "Succeeded.";   
        }
    catch (err)  {
        return "Failed: " + err;   
        }
    ';
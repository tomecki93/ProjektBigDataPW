create or replace schema META COMMENT='Schema to store process logs';

create or replace TABLE PROCESS_LOGS (
	LOG_ID NUMBER(38,0) NOT NULL autoincrement,
	JOB_ID VARCHAR(100),
	JOB_NAME VARCHAR(100),
	T_START_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	T_END_TIMESTAMP TIMESTAMP_NTZ(9)
);
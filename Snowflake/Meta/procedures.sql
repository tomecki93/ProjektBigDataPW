CREATE OR REPLACE PROCEDURE "META_END_PROCESS"("JOB_ID" VARCHAR(16777216), "JOB_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
    var sql_command_load = `UPDATE STORE_DB.META.PROCESS_LOGS SET T_END_TIMESTAMP = current_timestamp() WHERE T_END_TIMESTAMP is null AND JOB_ID = ''` + JOB_ID + `'' AND JOB_NAME = ''`+ JOB_NAME +`''`;
    try {
        snowflake.execute (
            {sqlText: sql_command_load}
            );
        return "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    ';
CREATE OR REPLACE PROCEDURE "META_START_PROCESS"("JOB_ID" VARCHAR(16777216), "JOB_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
STRICT
EXECUTE AS OWNER
AS '
    var sql_command_load = `INSERT INTO STORE_DB.META.PROCESS_LOGS (JOB_ID, JOB_NAME) VALUES (''` + JOB_ID + `'',''` + JOB_NAME + `'')`;

    try {
        snowflake.execute (
            {sqlText: sql_command_load}
            );
        return "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    ';
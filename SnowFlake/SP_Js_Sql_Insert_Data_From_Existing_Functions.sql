-- CALL DEV_DB.SCHEMA.SP_STG_INSERT_SOURCES()

CREATE OR REPLACE PROCEDURE DEV_DB.SCHEMA.SP_STG_INSERT_SOURCES()
RETURNS STRING
LANGUAGE JAVASCRIPT AS 
$$

try {

    -- Logs related variables
    var timeSrt, timeEnd, timeSrtDtl, timeEndDtl;
    var category = 'STG_INSERT_SOURCES_DTL', rowsOpr = 'INSERT', procName = Object.keys(this)[0];
    var rowsNmb = 0, rowsNmbTot = 0;
    var sqlLogs = `CALL DEV_DB.SCHEMA.SP_PRC_LOGS (:1,:2,:3,:4,:5,:6,'')`;
    timeSrt = new Date (new Date()).toLocaleString("pl-PL", {timeZone: "Europe/Warsaw"});
    
    -- Get the list of all functions in SCHEMA DB starting from F_STG_SRC
    var functionsDeck = [];
    var sqlCode = `SELECT FUNCTION_NAME FROM DEV_DB.INFORMATION_SCHEMA.FUNCTIONS WHERE FUNCTION_SCHEMA = 'SCHEMA' AND FUNCTION_NAME LIKE 'F_STG_SRC%';`
    var statement = snowflake.execute ({sqlText: sqlCode}); 
    while (statement.next()) { functionsDeck.push(statement.getColumnValue(1)); }   


    -- Recreate destination table TABLE_1
    sqlCode = `
        CREATE OR REPLACE TABLE DEV_DB.SCHEMA.TABLE_1 ( ID NUMBER(12,0) autoincrement, SOURCE VARCHAR(50), KPI_KEY VARCHAR(50), 
            SLS_MKT_KEY VARCHAR(50), MRU_KEY VARCHAR(50), ORU_KEY VARCHAR(50), COMM_TEAM VARCHAR(50), DATA_TYPE VARCHAR(50), TYPE VARCHAR(50), MONTHS NUMBER(2,0), 
            YEARS NUMBER(4,0), CURRENCY VARCHAR(50), VALUE NUMBER(38,6) );
    `
        snowflake.execute ({sqlText: sqlCode}); 


    -- Insert data from the functions one by one
    for (i = 0; i < functionsDeck.length; i++) {

        timeSrtDtl = new Date (new Date()).toLocaleString("pl-PL", {timeZone: "Europe/Warsaw"});

        sqlCode = `
            INSERT INTO DEV_DB.SCHEMA.TABLE_1 (SOURCE, KPI_KEY, SLS_MKT_KEY, MRU_KEY, ORU_KEY, COMM_TEAM, DATA_TYPE, TYPE, MONTHS, YEARS, CURRENCY, VALUE)
            SELECT SOURCE, KPI_KEY, SLS_MKT_KEY, MRU_KEY, ORU_KEY, COMM_TEAM, DATA_TYPE, TYPE, MONTHS, YEARS, CURRENCY, VALUE 
            FROM TABLE (` + functionsDeck[i] + `());
        `
            statement = snowflake.createStatement({sqlText: sqlCode});
            statement.execute();

            
            -- Logs after a single run of the insert statement
            timeEndDtl = new Date (new Date()).toLocaleString("pl-PL", {timeZone: "Europe/Warsaw"});
            rowsNmb = statement.getNumRowsAffected();
            rowsNmbTot += rowsNmb;
            sqlExec = snowflake.createStatement( { sqlText: sqlLogs, binds: [category, functionsDeck[i], timeSrtDtl, timeEndDtl, rowsNmb, rowsOpr] } );
            sqlExec.execute(); 
    }

    -- Logs when all inserts are finalized
    timeEnd = new Date (new Date()).toLocaleString("pl-PL", {timeZone: "Europe/Warsaw"}) + '.' + new Date().getMilliseconds();
    category = 'STG_INSERT_SOURCES';
    sqlExec = snowflake.createStatement( { sqlText: sqlLogs, binds: [category, procName, timeSrt, timeEnd, rowsNmbTot, rowsOpr] } );
    sqlExec.execute();
    
} catch (err) {
    -- Logs when fail
    sqlLogs = `CALL DEV_DB.SCHEMA.SP_PRC_LOGS (:1,:2,:3,:4, 0,:5,:6)`; 
    timeEnd = new Date (new Date()).toLocaleString("pl-PL", {timeZone: "Europe/Warsaw"});
    var errMessage = procName + ' - ' + functionsDeck[i] + ' - Error: ' + err.message;
    var errCode = err.code;
    sqlExec = snowflake.createStatement( { sqlText: sqlLogs, binds: [category, errMessage, timeSrt, timeEnd, rowsOpr, errCode] } );
    sqlExec.execute(); 
    var result = "Failed: Code: " + err.code + "\n  State: " + err.state + "\n  Message: " + err.message + "\n Stack Trace: \n" + err.stackTraceTxt;
    return result
}

$$
;     



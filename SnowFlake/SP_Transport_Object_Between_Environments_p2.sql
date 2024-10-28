CREATE OR REPLACE PROCEDURE DB.SCHEMA.SP_TRANSPORT_ENTIRE_SCHEMA_STP2_RELEASE("SRC_DATABASE" VARCHAR(16777216), "SRC_SCHEMA" VARCHAR(16777216), "TGT_DATABASE" VARCHAR(16777216), "TGT_SCHEMA" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '

    try {

        var srcDb = SRC_DATABASE;
        var srcSchem = SRC_SCHEMA;
        var tgtDb = TGT_DATABASE;
        var tgtSchem = TGT_SCHEMA;
        var objNames = [];
        var objIds = [];
        var objDdls = [];
        var transpEnd = 0; var nmbFailedDdls = 0; var nmbPrevFailedDdls = 0; var errors = ''''; var msg = ''''
        var sqlCmd = ''''; var sqlCmd = '''';
        

        // CLONE TABLES IN TGT ENVIRONMENT BEFORE REPLACING THEM
        // Makes copies of all tables in target schema, for which the definitions in source and target schemas are different. Adds _TRANS_COPY_ prefix to table clones
        
        sqlCmd = `
            SELECT OBJ_NAME
            FROM DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST
            WHERE OBJ_TYPE = ''TABLE'' AND IS_SRC = IS_TGT AND IS_TO_TRANSPORT = 1
            ORDER BY ID
        `;
            statement = snowflake.execute ({sqlText: sqlCmd});
            while (statement.next()) { objNames.push(statement.getColumnValue(1)); };

            
        for (i = 0; i < objNames.length; i++) {
            sqlCmd = `
                CREATE OR REPLACE TABLE ` + tgtDb + `.` + tgtSchem + `._TRANS_COPY_` + objNames[i] + `
                CLONE ` + tgtDb + `.` + tgtSchem + `.` + objNames[i]
            statement = snowflake.execute ({sqlText: sqlCmd});
            msg = ''Clones of some tables in target environment had been created prior to their recreation according to the source environment definition. Check tables named as: _TRANS_COPY_%''
        };


        // TRANSPORT

        while(transpEnd <= 1) {    

            // REPLACE SRC SCHEMA TO TGT SCHEMA IN DDL DEFINITIONS AND TRANSPORT ALL THE OBJECTS
            // Gets object ids to array objIds and object definitions to array objDdls.
            // Variable nmbFailedDdls holds number of not transported objects that are supposed to be transported (IS_TO_TRANSPORT = 1)

            sqlCmd = `
                SELECT ID, REPLACE(REPLACE(FIN_DDL,''` + srcDb + `.'',''` + tgtDb + `.''),''` + srcSchem + `.'',''` + tgtSchem + `.'')
                FROM (
                    -- Functions and procedures - Replaces necessary fields in src ddl to run it in tgt schema
                    SELECT FIN_DDL, IS_SRC, IS_TO_TRANSPORT, ID, TRANSPORT_STATUS FROM (
                        SELECT LEFT(SRC_DDL,POSITION(''RETURNS'',SRC_DDL)-2) AS SP_DEF, 
                            REPLACE(SP_DEF,''"'','''') AS SP_REP,
                            LEFT(OBJ_NAME,POSITION(''('',OBJ_NAME)-2) AS OBJ_NM,
                            REPLACE(SP_REP,OBJ_NM,''` + tgtDb + `.` + tgtSchem + `.'' || OBJ_NM) AS SP_TGT,
                            REPLACE(SRC_DDL,SP_DEF,SP_TGT) AS FIN_DDL,
                        SRC_DDL, IS_SRC, IS_TO_TRANSPORT, ID, TRANSPORT_STATUS
                        FROM DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST
                        WHERE OBJ_TYPE IN (''PROCEDURE'',''FUNCTION'')
                    )
        
                    UNION ALL 

                    -- Other objects - Replaces necessary fields in src ddl to run it in tgt schema
                    SELECT REPLACE(SRC_DDL,OBJ_NAME,''` + tgtDb + `.` + tgtSchem + `.'' || OBJ_NAME) AS FIN_DDL, IS_SRC, IS_TO_TRANSPORT, ID, TRANSPORT_STATUS
                    FROM DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST
                    WHERE OBJ_TYPE NOT IN (''PROCEDURE'',''FUNCTION'')
                ) 
                WHERE IS_SRC = 1 AND IS_TO_TRANSPORT = 1 AND TRANSPORT_STATUS IS NULL
                ORDER BY ID
            `;
                
                statement = snowflake.execute({sqlText: sqlCmd});
                nmbFailedDdls = statement.getNumRowsAffected();
                while (statement.next()) { objIds.push(statement.getColumnValue(1)); objDdls.push(statement.getColumnValue(2)); };
      
        
            // TRANSPORT OBJECTS EXECUTING SRC DDL DEFINITIONS
            // Runs all ddls in objDdls array to create objects in target schema and sets TRANSPORT_STATUS in T_TRANSPORT_OBJECTS_LIST. 
            // Repeates entire process until nmbPrevFailedDdls (nmbFailedDdls from previous run) is equal to nmbFailedDdls from actual run.
            // It is done to handle cases where e.g. one view depends on another view and they are not released in the correct order, so the 2nd view is missing when 1st view is being released.
            // The procedure repeatadly goes through objDdls as long as a single run ends up releasing any new object or all the objects get properly released.   

            for (j = 0; j < objDdls.length; j++) {
                sqlCmd = objDdls[j];
                try {   
                    updSql = `UPDATE DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST SET TRANSPORT_DDL = \\$\\$ ` + sqlCmd + ` \\$\\$ WHERE ID = ` + objIds[j];
                    updStatement = snowflake.execute ({sqlText: updSql});
                    
                    statement = snowflake.execute ({sqlText: sqlCmd});
                    
                    updSql = `UPDATE DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST SET TRANSPORT_STATUS = ''Success'' WHERE ID = ` + objIds[j];
                    updStatement = snowflake.execute ({sqlText: updSql});
                } catch(err) {
                    if (transpEnd == 1) { 
                        updSql = `UPDATE DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST SET TRANSPORT_STATUS = ''` + err.message.replaceAll("''","") + `'' WHERE ID = ` + objIds[j];
                        updStatement = snowflake.execute ({sqlText: updSql});
                        errors = 1;
                    }
                }
            }

            if(objDdls.length == 0 || nmbPrevFailedDdls == nmbFailedDdls) { transpEnd += 1; }
            nmbPrevFailedDdls = nmbFailedDdls
        };

        if (errors == 1) {
            return `Some objects have not been transported successfully: 
            SELECT OBJ_NAME, TRANSPORT_STATUS, TRANSPORT_DDL 
            FROM DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST
            WHERE TRANSPORT_STATUS <> ''Success'' OR TRANSPORT_STATUS IS NULL AND IS_TO_TRANSPORT = 1
            ORDER BY ID; \\n` + msg
        } else {
            return `Transport successfull \\n` + msg;
        }
    
    } catch(err) {
        return err + '' | '' + sqlCmd
    }
';
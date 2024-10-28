CREATE OR REPLACE PROCEDURE DB.SCHEMA.SP_TRANSPORT_ENTIRE_SCHEMA_STP1_PREPARATION("SRC_DATABASE" VARCHAR(16777216), "SRC_SCHEMA" VARCHAR(16777216), "TGT_DATABASE" VARCHAR(16777216), "TGT_SCHEMA" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '

    /*  PROCEDURE DESCRIPTION:
        Procedure retrieves definitions of the objects: procedures, functions, tables, views, file formats
        from SRC_DATABASE.SRC_SCHEMA and TGT_DATABASE.TGT_SCHEMA and places them in a T_TRANSPORT_OBJECTS_LIST table for comparison,
        basing on the assumption that compared schemas are Dev/Test/Prod environments and they contain similar or identical objects.
        Retrieved ddl definitions of all the objects are placed in columns SRC_DDL and TGT_DDL of T_TRANSPORT_OBJECTS_LIST table.
        The column IS_TO_TRANSPORT is automatically set to 0 if objects in both compared schemas are identical.
        Objects with IS_TO_TRANSPORT = 1 will be transported to target schema using SRC_DDL as their definition when SP_TRANSPORT_ENTIRE_SCHEMA_STP2_RELEASE is executed.
    */

    try {
    
        var srcDb = SRC_DATABASE;
        var srcSchem = SRC_SCHEMA;
        var tgtDb = TGT_DATABASE;
        var tgtSchem = TGT_SCHEMA;    
        var sqlCmd = ''''; var statement;
        var objNames = []; var objTypes = []; var objDdls = [];

       
        sqlCmd = `
            CREATE OR REPLACE TABLE DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST 
                (ID number(4,0), OBJ_TYPE varchar(50), OBJ_NAME varchar(200), OBJ_STATUS  varchar(200), IS_TO_TRANSPORT number(1,0), IS_SRC number(1,0),
                 IS_TGT number(1,0), SRC_DDL varchar(999999), TGT_DDL varchar(999999), TRANSPORT_DDL varchar(999999), TRANSPORT_STATUS varchar(200))
        `;
            statement = snowflake.createStatement({sqlText: sqlCmd});
            statement.execute();

    
        sqlCmd = `
            INSERT INTO DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST (ID, OBJ_TYPE, OBJ_NAME, OBJ_STATUS, IS_TO_TRANSPORT, IS_SRC, IS_TGT)
            WITH SRC_PRC_MAP AS (
                -- Get mapping of procedure/function and its arguments for SRC srcDb
                SELECT p.procedure_name, ''( ''|| listagg(split_part(trim(t.value),'' '',2), '', '') within group (order by t.index) || '')'' AS ARG
                FROM ` + srcDb + `.INFORMATION_SCHEMA.procedures as p, 
                    table(split_to_table(substring(p.argument_signature, 2,length(p.argument_signature)-2), '','')) t
                GROUP BY 1, t.seq
                    UNION ALL
                SELECT p.function_name, ''( ''|| listagg(split_part(trim(t.value),'' '',2), '', '') within group (order by t.index) || '')'' AS ARG
                FROM ` + srcDb + `.INFORMATION_SCHEMA.functions as p, 
                    table(split_to_table(substring(p.argument_signature, 2,length(p.argument_signature)-2), '','')) t
                GROUP BY 1, t.seq
            ), TGT_PRC_MAP AS (
                -- Get mapping of procedure/function and its arguments for SRC tgtDb
                SELECT p.procedure_name, ''( ''|| listagg(split_part(trim(t.value),'' '',2), '', '') within group (order by t.index) || '')'' AS ARG
                FROM ` + tgtDb + `.INFORMATION_SCHEMA.procedures as p, 
                    table(split_to_table(substring(p.argument_signature, 2,length(p.argument_signature)-2), '','')) t
                GROUP BY 1, t.seq
                    UNION ALL
                SELECT p.function_name, ''( ''|| listagg(split_part(trim(t.value),'' '',2), '', '') within group (order by t.index) || '')'' AS ARG
                FROM ` + tgtDb + `.INFORMATION_SCHEMA.functions as p, 
                    table(split_to_table(substring(p.argument_signature, 2,length(p.argument_signature)-2), '','')) t
                GROUP BY 1, t.seq
            ), SRC_OBJ AS (
                -- Get the list of all object at srcDb
                SELECT TABLE_NAME AS OBJ_NAME, REPLACE(TABLE_TYPE,''BASE '','''') AS OBJ_TYPE, CREATED, CASE WHEN TABLE_TYPE = ''VIEW'' THEN 3 ELSE 2 END AS ORD 
                FROM ` + srcDb + `.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = ''` + srcSchem + `'' 
                    UNION ALL 
                SELECT PROCEDURE_NAME, ''PROCEDURE'', CREATED, 5 AS ORD 
                FROM ` + srcDb + `.INFORMATION_SCHEMA.PROCEDURES 
                WHERE PROCEDURE_SCHEMA = ''` + srcSchem + `'' 
                    UNION ALL
                SELECT FUNCTION_NAME, ''FUNCTION'', CREATED, 4 AS ORD 
                FROM ` + srcDb + `.INFORMATION_SCHEMA.FUNCTIONS 
                WHERE FUNCTION_SCHEMA = ''` + srcSchem + `'' 
                    UNION ALL
                SELECT FILE_FORMAT_NAME, ''FILE_FORMAT'', CREATED, 1 AS ORD 
                FROM ` + srcDb + `.INFORMATION_SCHEMA.FILE_FORMATS 
                WHERE FILE_FORMAT_SCHEMA = ''` + srcSchem + `'' AND POSITION(''_TEMP_'',FILE_FORMAT_NAME) = 0
            ), TGT_OBJ AS (
                -- Get the list of all object at tgtDb
                SELECT TABLE_NAME AS OBJ_NAME, REPLACE(TABLE_TYPE,''BASE '','''') AS OBJ_TYPE 
                FROM ` + tgtDb + `.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = ''` + tgtSchem + `''
                    UNION ALL 
                SELECT PROCEDURE_NAME, ''PROCEDURE'' 
                FROM ` + tgtDb + `.INFORMATION_SCHEMA.PROCEDURES 
                WHERE PROCEDURE_SCHEMA = ''` + tgtSchem + `'' 
                    UNION ALL
                SELECT FUNCTION_NAME, ''FUNCTION'' 
                FROM ` + tgtDb + `.INFORMATION_SCHEMA.FUNCTIONS 
                WHERE FUNCTION_SCHEMA = ''` + tgtSchem + `'' 
                    UNION ALL
                SELECT FILE_FORMAT_NAME, ''FILE_FORMAT'' 
                FROM ` + tgtDb + `.INFORMATION_SCHEMA.FILE_FORMATS 
                WHERE FILE_FORMAT_SCHEMA = ''` + tgtSchem + `'' AND POSITION(''_TEMP_'',FILE_FORMAT_NAME) = 0 
            ), SRC_OBJ_MAP AS (
                -- Map arguments with procedure/function names for SRC_OBJ
                SELECT OBJ.OBJ_TYPE, OBJ.OBJ_NAME || IFNULL(ARG.ARG,'''') AS OBJ_NAME, CREATED, ORD
                FROM SRC_OBJ OBJ
                LEFT JOIN SRC_PRC_MAP ARG ON OBJ.OBJ_NAME = ARG.PROCEDURE_NAME
            ), TGT_OBJ_MAP AS (
                -- Map arguments with procedure/function names for TGT_OBJ
                SELECT OBJ.OBJ_TYPE, OBJ.OBJ_NAME || IFNULL(ARG.ARG,'''') AS OBJ_NAME
                FROM TGT_OBJ OBJ
                LEFT JOIN TGT_PRC_MAP ARG ON OBJ.OBJ_NAME = ARG.PROCEDURE_NAME
            )
                SELECT ROW_NUMBER() OVER(ORDER BY IS_TO_TRANSPORT, ORD, CREATED) AS ID, OBJ_TYPE, OBJ_NAME, OBJ_STATUS, IS_TO_TRANSPORT, IS_SRC, IS_TGT 
                FROM (
                    -- Checks in objects exist in both srcDb and tgtDb or in srcDb only
                    SELECT SRC.OBJ_TYPE, SRC.OBJ_NAME, 
                        CASE WHEN TGT.OBJ_NAME IS NULL 
                            THEN ''Exists only in ` + srcDb + `.` + srcSchem + `'' 
                            ELSE ''Different definitions in both environments'' 
                        END AS OBJ_STATUS,
                        1 AS IS_TO_TRANSPORT, 1 AS IS_SRC, CASE WHEN TGT.OBJ_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_TGT, CREATED, ORD
                    FROM SRC_OBJ_MAP SRC
                    LEFT JOIN TGT_OBJ_MAP TGT ON SRC.OBJ_NAME = TGT.OBJ_NAME AND SRC.OBJ_TYPE = TGT.OBJ_TYPE
                        
                        UNION ALL
                    
                    -- Adds to the list definitions of the objects existing only in tgtDb
                    SELECT TGT.OBJ_TYPE, TGT.OBJ_NAME, ''Exists only in ` + tgtDb + `.` + tgtSchem + `'' AS OBJ_STATUS, 
                        0 AS IS_TO_TRANSPORT, 0 AS IS_SRC, 1 AS IS_TGT, NULL AS CREATED, NULL AS ORD
                    FROM SRC_OBJ_MAP SRC
                    RIGHT JOIN TGT_OBJ_MAP TGT ON SRC.OBJ_NAME = TGT.OBJ_NAME AND SRC.OBJ_TYPE = TGT.OBJ_TYPE
                    WHERE SRC.OBJ_NAME IS NULL  
                )
        `;
          
            statement = snowflake.createStatement({sqlText: sqlCmd});
            statement.execute();

            
        // GET SRC DDLS INTO T_TRANSPORT_OBJECTS_LIST TABLE
        // Loops through all the rows in the table, gets ddl definition, puts the the definition into a separate column in the table next to the object name
                
        sqlCmd = `SELECT OBJ_TYPE, OBJ_NAME FROM DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST WHERE IS_SRC = 1`;
            statement = snowflake.execute ({sqlText: sqlCmd});
            while (statement.next()) { objTypes.push(statement.getColumnValue(1)); objNames.push(statement.getColumnValue(2)); };

        for (i = 0; i < objTypes.length; i++) {
            sqlCmd = `SELECT GET_DDL(''` + objTypes[i] + `'',''` + srcDb + `.` + srcSchem + `.` + objNames[i] + `'')`
            statement = snowflake.execute ({sqlText: sqlCmd});
            while (statement.next()) { objDdls.push(statement.getColumnValue(1)) };
        }

        for (j = 0; j < objNames.length; j++) {
            sqlCmd = `UPDATE DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST SET SRC_DDL = \\$\\$ ` + objDdls[j] + ` \\$\\$ WHERE OBJ_NAME = ''` + objNames[j] + `''`
            statement = snowflake.execute ({sqlText: sqlCmd});
        }

        
        // GET TGT DDLS INTO T_TRANSPORT_OBJECTS_LIST TABLE
        // Loops through all the rows in the table, gets ddl definition, puts the the definition into a separate column in the table next to the object name

        objNames = []; objTypes = []; objDdls = [];
        
        sqlCmd = `SELECT OBJ_TYPE, OBJ_NAME FROM DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST WHERE IS_TGT = 1`;
            statement = snowflake.execute ({sqlText: sqlCmd});
            while (statement.next()) { objTypes.push(statement.getColumnValue(1)); objNames.push(statement.getColumnValue(2)); };

        for (k = 0; k < objTypes.length; k++) {
            sqlCmd = `SELECT GET_DDL(''` + objTypes[k] + `'',''` + tgtDb + `.` + tgtSchem + `.` + objNames[k] + `'')`
            statement = snowflake.execute ({sqlText: sqlCmd});
            while (statement.next()) { objDdls.push(statement.getColumnValue(1)); };
            
        }

        for (l = 0; l < objNames.length; l++) {
            sqlCmd = `UPDATE DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST SET TGT_DDL = \\$\\$ ` + objDdls[l] + ` \\$\\$ WHERE OBJ_NAME = ''` + objNames[l] + `''`
            statement = snowflake.execute ({sqlText: sqlCmd});
        }


        // COMPARE SRC AND TGT DDL AND EXCLUDE IDENTICAL OBJECTS FROM THE TRANSPORT
        
        sqlCmd = `
            UPDATE DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST
            SET IS_TO_TRANSPORT = 0, OBJ_STATUS = ''Same definitions in both environments''  
            WHERE IS_SRC = IS_TGT AND UPPER(SRC_DDL) = UPPER(REPLACE(REPLACE(TGT_DDL,''` + tgtDb + `.'',''` + srcDb + `.''),''` + tgtSchem + `.'',''` + srcSchem + `.''));
        `;
            statement = snowflake.execute ({sqlText: sqlCmd});

        sqlCmd = `
            UPDATE DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST upd
            SET upd.ID = src.NEW_ID
            FROM (
                SELECT ID, ROW_NUMBER() OVER(ORDER BY IS_TO_TRANSPORT DESC, ID) AS NEW_ID
                FROM DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST
            ) src
            WHERE upd.ID = src.ID;
        `;
            statement = snowflake.execute ({sqlText: sqlCmd});

            
        return `Analyze the results of below query before the release: \\n 
                SELECT * FROM DB.SCHEMA.T_TRANSPORT_OBJECTS_LIST ORDER BY IS_TO_TRANSPORT DESC, OBJ_TYPE, OBJ_STATUS, OBJ_NAME; \\n\\n
                SET IS_TO_TRANSPORT = 0 to remove objects from the transport and proceed by running the procedure: \\n
                DB.SCHEMA.SP_TRANSPORT_ENTIRE_SCHEMA_STP2_RELEASE;`;
    
    } catch(err) {
        return err
    }
';
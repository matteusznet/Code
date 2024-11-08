--CALL DB.SCHEMA.SP_PY_TURN_XLS_INTO_TABLE('TEST_STG');

CREATE OR REPLACE PROCEDURE DB.SCHEMA.SP_PY_TURN_XLS_INTO_TABLE(stage varchar(400))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python','openpyxl','pandas')
HANDLER = 'main'
EXECUTE AS CALLER
AS '

import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import re
import openpyxl

def main(session: snowpark.Session, stage):

    # list all the files belonging to the stage specified in the sp parameter
    stageFiles = session.sql("LIST @DEV_DB.DECKS_RAW.TEST_STG;")

    # run through all stageFiles
    for row in stageFiles.collect():
        filePath = row[''name'']
        
        # removing stage name from the file path e.g: test_stg/fileName.xlsx
        fileNameSplit = filePath.split(''/'')
        fileNameSplit = fileNameSplit[-1]

        # defining file name and extension from file path
        fileNameSplit = fileNameSplit.split(''.'')
        fileExtension = ''.'' + fileNameSplit[-1]
        fileName = fileNameSplit[0].split(''.'')[0]
        
        try:
            with SnowflakeFile.open(''@'' + filePath, ''rb'', require_scoped_url=False) as f:
                if fileExtension in (''.xlsx'', ''.xlsm''):
                    df = pd.read_excel(f.read(), sheet_name=None, keep_default_na=False, na_values="NULL", dtype=None)
                    
                    # run through all sheets in th excel file
                    for sheet, data in df.items():
                        data.columns = data.columns.astype(str).str.upper().str.replace('' '', ''_'')

                        # add metadata columns to sheet content and create a dataframe 
                        data[''ID_COL''] = data.reset_index().index + 1
                        data[''FILENAME''] = fileName
                        excelDf = session.create_dataframe(df[sheet].astype(str))

                        # create table
                        sheet = re.sub(r''[^a-zA-Z0-9]'',''_'',sheet)
                        tableName = re.sub(r''[^a-zA-Z0-9]'', ''_'', ''TEST_''+fileName+''_''+sheet)
                        excelDf.write.mode("overwrite").save_as_table(tableName)
                
        except Exception as e:        
            error_message = str(e).replace("''''", "_")  
            return error_message

    return excelDf

';
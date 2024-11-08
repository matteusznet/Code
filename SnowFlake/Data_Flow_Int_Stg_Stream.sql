-- Create stage
USE DEV_DB.DECKS_RAW;

CREATE STAGE TEST_STG
	DIRECTORY = ( ENABLE = true );

    
-- Create stream based on the stage
CREATE OR REPLACE STREAM DEV_DB.DECKS_RAW.TEST_STREAM on directory(@TEST_STG);


-- Select from stream
SELECT * FROM DEV_DB.DECKS_RAW.TEST_STREAM;


-- Check if the stream has data = file has been loaded to the stage
SELECT system$stream_has_data('TEST_STREAM');


-- Consume the stream SP by inserting data to T_TEMP table
CREATE OR REPLACE PROCEDURE DEV_DB.DECKS_RAW.TEST ()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN 
    IF ((SELECT system$stream_has_data('TEST_STREAM')) = TRUE) THEN
    
        CREATE OR REPLACE TEMPORARY TABLE T_TEMP (RELATIVE_PATH VARCHAR(200), creation_date TIMESTAMP_NTZ(9));
    
        INSERT INTO T_TEMP 
        SELECT RELATIVE_PATH, CONVERT_TIMEZONE('Europe/Warsaw', CURRENT_TIMESTAMP)::TIMESTAMP_NTZ(9)
        FROM DEV_DB.DECKS_RAW.TEST_STREAM
        WHERE RELATIVE_PATH IS NOT NULL AND METADATA$ACTION = 'INSERT';
    ELSE
        RETURN 'Stream is empty';
    END IF;
END;
$$
;

CALL DEV_DB.DECKS_RAW.TEST();

SELECT * FROM T_TEMP;


-- Task checking every 5 minutes if stream is not empty = a file was uploaded to the stage
CREATE OR REPLACE task DEV_DB.DECKS_RAW.TEST_TASK
	warehouse=PROD_WH_TASK_S
	schedule='5 MINUTE'
	when SYSTEM$STREAM_HAS_DATA('TEST_STREAM')
AS
    CALL DEV_DB.DECKS_RAW.TEST();

-- Activate/dectivate task
EXECUTE TASK DEV_DB.DECKS_RAW.TEST_TASK;
ALTER TASK DEV_DB.DECKS_RAW.TEST_TASK RESUME;
ALTER TASK DEV_DB.DECKS_RAW.TEST_TASK SUSPEND;
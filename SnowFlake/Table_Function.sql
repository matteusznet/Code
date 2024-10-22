CREATE OR REPLACE FUNCTION F_STG_SRC_SYSTEM_QUERIES()
RETURNS TABLE (
	SOURCE VARCHAR(50),
	KPI_KEY VARCHAR(50),
    SLS_MKT_KEY VARCHAR(50),
	MRU_KEY VARCHAR(50),
	ORU_KEY VARCHAR(50),
	COMM_TEAM VARCHAR(50),
    DATA_TYPE VARCHAR(50),
    TYPE VARCHAR(50),
    MONTHS NUMBER(2,0),
    YEARS NUMBER(4,0),
	CURRENCY VARCHAR(50),
	VALUE NUMBER(38,6)
) AS
$$
    -- Source Table
    SELECT SOURCE, KPI_KEY, SLS_MKT_KEY, MRU_KEY, ORU_KEY, COMM_TEAM, DATA_TYPE, TYPE, MONTHS, YEARS, CURRENCY, VALUE
    FROM DEV_DB.SCHEMA.TABLE_1
$$ 
;


SELECT * 
FROM TABLE (DEV_DB.SCHEMA.F_STG_SRC_SYSTEM_QUERIES())
;
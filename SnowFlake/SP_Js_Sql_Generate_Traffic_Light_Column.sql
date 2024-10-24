-- CALL DEV_DB.SCHEMA.SP_SAC_TRAFFIC_LIGHTS('T_SAC_MPR_MARKETS')


CREATE OR REPLACE PROCEDURE DEV_DB.SCHEMA.SP_SAC_TRAFFIC_LIGHTS (tableName string)
RETURNS STRING
LANGUAGE JAVASCRIPT AS 
$$
    -- Variables used for logs 
    var result = ''; 
    var timeSrt, timeEnd, rowsNmb = 0;
    const rowsOpr = 'INSERT', category = 'SAC_SOURCES', procName = Object.keys(this)[0];
    var sqlLogs = `CALL DEV_DB.SCHEMA.SP_PRC_LOGS (:1,:2,:3,:4,:5,:6,'')`;
    timeSrt = new Date (new Date()).toLocaleString("pl-PL", {timeZone: "Europe/Warsaw"}); 
    
    -- Retrieving SP parameter tableName to var tableName
    var tableName = TABLENAME;
    var tlCalcList = [], tlCalcCol1 = [], tlCalcCol2 = [];
    var monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

    -- Get reporting month and quarter to variables mth and qrt
    var timeVariables = snowflake.execute ({sqlText: `SELECT REP_MONTH, REP_QUARTER FROM DEV_DB.SCHEMA.V_PRC_VARIABLES`});
    while (timeVariables.next())  {
        var mth = timeVariables.getColumnValue(1);
        mth = monthNames[mth-1];
        var qrt = timeVariables.getColumnValue(2);
    }
    
    -- Definition of compared columns
    -- tlCalcList - name of column in DEV_DB.SCHEMA.T_MAP_SAC_DEF_ROWS definition table. If column's value is 1 or -1 TL is to be added
    -- tlCalcCol1 - name of first SAC_COLUMN_KEY value from DEV_DB.SCHEMA.[tableName] to be used in comparison
    -- tlCalcCol2 - name of second SAC_COLUMN_KEY value from DEV_DB.SCHEMA.[tableName] to be used in comparison
    tlCalcList[0] = 'TL_ITM_ACTvsFC';     tlCalcCol1[0] = mth + ' Act CY';          tlCalcCol2[0] = mth + ' FC CY';
    tlCalcList[1] = 'TL_ITQ_ACTvsPFC';    tlCalcCol1[1] = 'Q' + qrt + ' Act CY';    tlCalcCol2[1] = 'Q' + qrt + ' PrevFC CY';
    tlCalcList[2] = 'TL_ITQ_ACTvsTGT';    tlCalcCol1[2] = 'Q' + qrt + ' Act CY';    tlCalcCol2[2] = 'Q' + qrt + ' Tgt CY';
    tlCalcList[3] = 'TL_YTD_ACTvsTGT';    tlCalcCol1[3] = 'YTD Act CY';             tlCalcCol2[3] = 'YTQ Tgt CY';
    tlCalcList[4] = 'TL_FY_FCvsTGT';      tlCalcCol1[4] = 'FY FC CY';               tlCalcCol2[4] = 'FY Tgt CY';
    tlCalcList[5] = 'TL_LY_Q4_ACTvsPFC';  tlCalcCol1[5] = 'Q4 Act LY';              tlCalcCol2[5] = 'Q4 PreFC LY';
    tlCalcList[6] = 'TL_LY_Q4_ACTvsTGT';  tlCalcCol1[6] = 'Q4 Act LY';              tlCalcCol2[6] = 'Q4 TGT LY';
    
    -- Looping through all Traffic Lights defined relations: tlCalcList
    for (j=0; j<tlCalcList.length; j++) {
        
        -- Dynamic sql depending on the tableName, defined TL relation tlCalcList and 2 compared SAC_COLUMN_KEYs: tlCalcCol1, tlCalcCol2 
        var sqlCode = `
            INSERT INTO DEV_DB.SCHEMA.` + tableName + ` (KPI_KEY, COMM_TEAM, MRU_KEY, MRU_NAME, ORU_KEY, ORU_NAME, DATA_TYPE, TYPE, YEARS, MONTHS, CURRENCY, 
                SAC_PROMPT_DATE, SAC_AUTHORIZATION, SAC_OBJECT_KEY, SAC_OBJECT_NAME, SAC_KPI_KEY, SAC_KPI_NAME, SAC_COLUMN_KEY, SAC_COLUMN_NAME, VALUE)
            SELECT KPI_KEY, COMM_TEAM, MRU_KEY, MRU_NAME, ORU_KEY, ORU_NAME, DATA_TYPE, TYPE, YEARS, MONTHS, CURRENCY, 
                SAC_PROMPT_DATE, SAC_AUTHORIZATION, SAC_OBJECT_KEY, SAC_OBJECT_NAME, SAC_KPI_KEY, SAC_KPI_NAME, SAC_COLUMN_KEY, SAC_COLUMN_NAME,
                CASE WHEN "'V2'" > 0 THEN
                    CASE WHEN TL_SIGN * ("'V2'" - "'V1'") / "'V2'" <= 0 THEN 1
                        WHEN (TL_SIGN * ("'V2'" - "'V1'") / "'V2'" > 0) AND (TL_SIGN * ("'V2'" - "'V1'") / "'V2'" <= 0.05) THEN 0
                        WHEN (TL_SIGN * ("'V2'" - "'V1'") / "'V2'" > 0.05) THEN -1
                    END
                WHEN "'V2'" < 0 then
                    CASE WHEN TL_SIGN * ("'V2'" - "'V1'") / "'V2'" >= 0 THEN 1
                        WHEN (TL_SIGN * ("'V2'" - "'V1'") / "'V2'" < 0) and (TL_SIGN * ("'V2'" - "'V1'") / "'V2'" >= -0.05) THEN 0
                        WHEN (TL_SIGN * ("'V2'" - "'V1'") / "'V2'" < -0.05) THEN -1
                    END
                WHEN "'V2'" = 0 then
                    CASE WHEN TL_SIGN * ("'V2'" - "'V1'") > 0 THEN -1
                        WHEN ("'V2'" - "'V1'") = 0 THEN -1 * TL_SIGN
                        WHEN TL_SIGN * ("'V2'" - "'V1'") < 0	THEN 1
                    END
                END AS VALUE
            FROM (
                SELECT KPI_KEY, dck.COMM_TEAM, MRU_KEY, MRU_NAME, dck.ORU_KEY, ORU_NAME, 'TL' AS DATA_TYPE, TYPE, YEARS, MONTHS, CURRENCY, 
                    SAC_PROMPT_DATE, SAC_AUTHORIZATION, dck.SAC_OBJECT_KEY, dck.SAC_OBJECT_NAME, dck.SAC_KPI_KEY, dck.SAC_KPI_NAME, 
                    -- Gets min SAC_COLUMN_KEY from 2 compared and changes last number of the KEY to 5
                    -- E.g. compared SAC_COLUMN_KEYs are: 1000450, 1000460, final SAC_COLUMN_KEY = 1000455
                    -- Thus when we order the data by SAC_COLUMN_KEYs, newly created SAC_COLUMN_KEY is between copmared SAC_COLUMN_KEYs
                    CONCAT(LEFT(MIN(SAC_COLUMN_KEY) OVER (PARTITION BY ORU_KEY, MRU_KEY, dck.SAC_KPI_KEY ORDER BY SAC_COLUMN_KEY),6),5) AS SAC_COLUMN_KEY, 
                    '` + tlCalcList[j] + `' AS SAC_COLUMN_NAME, ` + tlCalcList[j] + ` AS TL_SIGN,
                    CASE SAC_COLUMN_NAME WHEN '` + tlCalcCol1[j] + `' THEN 'V1' WHEN '` + tlCalcCol2[j] + `' THEN 'V2' END AS COL, 
                    VALUE 
                FROM DEV_DB.SCHEMA.` + tableName + ` dck
                INNER JOIN DEV_DB.SCHEMA.T_MAP_SAC_DEF_ROWS rws ON dck.SAC_KPI_KEY = rws.SAC_KPI_KEY
                WHERE SAC_COLUMN_NAME IN ('` + tlCalcCol1[j] + `','` + tlCalcCol2[j] + `')
                    AND SAC_PROMPT_DATE = (SELECT LAST_DAY(DATE_FROM_PARTS(REP_YEAR,REP_MONTH,1)) FROM DEV_DB.SCHEMA.V_PRC_VARIABLES)
                    AND ` + tlCalcList[j] + ` IS NOT NULL
            )
            -- Pivots Value column to V1, V2 columns base on COL defined above as COL
            -- Basically splits compared SAC_COLUMN_NAMEs to separate measures that are to the calaculations
            PIVOT (MAX(VALUE) FOR COL IN ('V1','V2'))
            ORDER BY ORU_KEY, SAC_KPI_KEY
        `
        
        statement = snowflake.createStatement({sqlText: sqlCode});
        statement.execute();
        rowsNmb = rowsNmb + statement.getNumRowsInserted();
    }

    -- LOGS
    timeEnd = new Date (new Date()).toLocaleString("pl-PL", {timeZone: "Europe/Warsaw"});
    sqlExec = snowflake.createStatement( { sqlText: sqlLogs, binds: [category, procName, timeSrt, timeEnd, rowsNmb, rowsOpr] } );
    sqlExec.execute();
    
    return result;
$$
;

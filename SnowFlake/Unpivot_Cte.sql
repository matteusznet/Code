 WITH IWD_Total_Signify AS (				
                                
    -- Separating measures based on different Select Case conditions
    SELECT YEAR, MONTH, indicator, business, business_name, owner_key, mru,
        CASE WHEN ri_group = '1. Sales' AND indicator = 'Target' THEN actual_eur ELSE 0 END AS Sales_Target_TPW,
        CASE WHEN ri_group = '1. Sales' AND indicator = 'Actuals' THEN actual_eur ELSE 0 END AS Sales_TPW,
        CASE WHEN ri_group = '2. IWD Costs' AND indicator = 'Target' THEN actual_eur ELSE 0 END AS IWD_Target_TPW,
        CASE WHEN ri_group = '2. IWD Costs' AND indicator = 'Actuals' THEN actual_eur ELSE 0 END AS Sellex_TPW,
        CASE WHEN ri_group = '2. IWD Costs' AND indicator = 'Actuals' AND functional_area_name ='Manufacturing' THEN actual_eur ELSE 0 END AS COGS_TPW,
        CASE WHEN ri_group = '2. IWD Costs' AND indicator = 'Actuals' AND functional_area_name IN ('Manufacturing', 'Commercial') THEN actual_eur ELSE 0 END AS IWD_Total_TPW,
        CASE WHEN ri_group = '2. IWD Costs' AND indicator = 'Target' AND functional_area_name ='Commercial' THEN actual_eur ELSE 0 END AS Sellex_Target_TPW,
        CASE WHEN ri_group = '2. IWD Costs' AND indicator = 'Target' AND functional_area_name ='Manufacturing' THEN actual_eur ELSE 0 END AS COGS_Tartet_TPW
    FROM PROD_DB.SCHEMA.TABLE_1 iwd
    INNER JOIN DEV_DB.DECKS_RAW.V_PRC_VARIABLES var ON iwd.YEAR = var.REP_YEAR
    WHERE scope = 1
        and ri_group IN ('1. Sales','2. IWD Costs')
        and indicator IN ('Target','Actuals')
        and scope_oru_copper = 1
        
), CTE_GRP AS (

    -- Aggregation SLS_MKT_KEY level N-0
    SELECT 'Snowflake_IWD' AS SOURCE, 'S00100' AS SLS_MKT_KEY, 'PD0100' AS MRU_KEY, 'L00100' AS ORU_KEY, 'ALL' AS COMM_TEAM, 'Actual' AS DATA_TYPE, 
        'ITM' AS TYPE, MONTH AS MONTHS, YEAR AS YEARS, 'EUR' AS CURRENCY,
        SUM(Sales_Target_TPW) AS Sales_Target_TPW, SUM(Sales_TPW) AS Sales_TPW, SUM(IWD_Target_TPW) AS IWD_Target_TPW, SUM(Sellex_TPW) AS Sellex_TPW, 
        SUM(COGS_TPW) AS COGS_TPW, SUM(IWD_Total_TPW) AS IWD_Total_TPW, SUM(Sellex_Target_TPW) AS Sellex_Target_TPW, SUM(COGS_Tartet_TPW) AS COGS_Tartet_TPW
    FROM IWD_Total_Signify
    GROUP BY MONTH, YEAR
    
    UNION ALL

    -- Aggregation SLS_MKT_KEY level N-2
    SELECT 'Snowflake_IWD' AS SOURCE, CONCAT('S', business) AS SLS_MKT_KEY, 'PD0100' AS MRU_KEY, 'L00100' AS ORU_KEY, 'ALL' AS COMM_TEAM, 'Actual' AS DATA_TYPE, 
        'ITM' AS TYPE, MONTH AS MONTHS, YEAR AS YEARS, 'EUR' AS CURRENCY,
        SUM(Sales_Target_TPW) AS Sales_Target_TPW, SUM(Sales_TPW) AS Sales_TPW, SUM(IWD_Target_TPW) AS IWD_Target_TPW, SUM(Sellex_TPW) AS Sellex_TPW, 
        SUM(COGS_TPW) AS COGS_TPW, SUM(IWD_Total_TPW) AS IWD_Total_TPW, SUM(Sellex_Target_TPW) AS Sellex_Target_TPW, SUM(COGS_Tartet_TPW) AS COGS_Tartet_TPW
    FROM IWD_Total_Signify
    WHERE business_name in ('SLS_MKT_1','SLS_MKT_2','SLS_MKT_3') 
    GROUP BY MONTH, YEAR, business
)

    -- Unpivot the measures. Values go to VALUE column, former colum names become KPI_KEY
    SELECT SOURCE, KPI_KEY, SLS_MKT_KEY, MRU_KEY, ORU_KEY, COMM_TEAM, DATA_TYPE, TYPE, MONTHS, YEARS, CURRENCY, CAST(ROUND(VALUE,6) as NUMBER(38,6)) AS VALUE
    FROM CTE_GRP
    UNPIVOT (VALUE FOR KPI_KEY IN (Sales_Target_TPW, Sales_TPW, IWD_Target_TPW, Sellex_TPW, COGS_TPW, IWD_Total_TPW, Sellex_Target_TPW, COGS_Tartet_TPW))
    
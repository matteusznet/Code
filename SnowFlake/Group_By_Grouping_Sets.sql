WITH CTE_KF AS (

    SELECT Year_Period, P_Bu, P_Bg, R_Subgroup, R_Country, R_Market, R_Mg, Commercial_team, Commercial_subteam, 
        CASE REPLACE(REPLACE(Data_Type,'RoFo(0','FC '),')','') WHEN 'AOP' THEN 'TGT' WHEN ACT_FC THEN 'FC' WHEN WRK_FC THEN 'WorkFC' ELSE Data_Type END AS Data_Type,
        CASE WHEN FSI_06 = 'TS100MONTHLYRESTRUCTURING'     THEN 'Adjustments_Restructuring' 
             WHEN FSI_KEY = '262134' AND FA_KEY = '2000'   THEN 'Amortization_Intangible_FA'
             ELSE RI_01 
        END AS KPI, 
        VALUE_IN_K_EUR, VALUE_IN_K_USD
    FROM DEV_DB.DECKS_RAW.KEY_FINANCIAL_DATA_UNPVT 
    CROSS JOIN (SELECT REP_MONTH, REP_YEAR, ACT_FC, WRK_FC FROM DEV_DB.DECKS_RAW.V_PRC_VARIABLES) var
    WHERE IS_CO = 'YYY'
        AND P_BG RLIKE 'BS900[1-3]'
        AND YEAR >= REP_YEAR-1
        AND DATA_TYPE IN ('Actual','AOP',CONCAT(REPLACE(ACT_FC,'FC ','RoFo(0'),')'),CONCAT(REPLACE(WRK_FC,'FC ','RoFo(0'),')'))
        AND NOT (DATA_TYPE = 'Actual' AND YEAR = REP_YEAR AND PERIOD > REP_MONTH)

), CTE_GRP AS (

    -- Grouping data on different Market Hierarchy levels
    SELECT kpi.Source, Year_Period, kpi.Kpi_Key_Deck, Data_Type, MAX(kpi.Mru_level) AS Mru_level, MAX(kpi.Oru_level) AS Oru_level, kpi.Value_scale,
        COALESCE(P_Bu, P_Bg, 'PD0100') AS Mru, COALESCE(R_Subgroup, R_Country, R_Market, R_Mg, 'L00100') AS Oru,
        IFF(Commercial_subteam IS NULL, Commercial_team, CONCAT(Commercial_team, '_', Commercial_subteam)) AS Comm_Team,
        SUM(VALUE_IN_K_EUR) AS Value_EUR, SUM(VALUE_IN_K_USD) AS Value_USD
    FROM CTE_KF kf
    INNER JOIN DEV_DB.DECKS_RAW.T_MAP_STG_KPI kpi ON kpi.SOURCE = 'KeyFinancialsCommTeam' AND kf.KPI = kpi.KPI_Key_Src
    GROUP BY GROUPING SETS (
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, R_Mg),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, R_Market),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, R_Country),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, R_Subgroup),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, R_Market, Commercial_team),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, R_Market, Commercial_team, Commercial_subteam),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, P_Bg),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, P_Bg, R_Mg),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, P_Bg, R_Market),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, P_Bg, R_Country),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, P_Bg, R_Subgroup),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, P_Bg, R_Market, Commercial_team),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, P_Bg, R_Market, Commercial_team, Commercial_subteam),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, P_Bu),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, P_Bu, R_Mg),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, P_Bu, R_Market),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, P_Bu, R_Country),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, P_Bu, R_Subgroup),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, P_Bu, R_Market, Commercial_team),
        (kpi.Kpi_Key_Deck, kpi.Source, kpi.Value_scale, kpi.Mru_level, kpi.Oru_level, Year_Period, Data_type, P_Bu, R_Market, Commercial_team, Commercial_subteam)
    )
    HAVING ORU NOT RLIKE '[0-9].*' AND ORU NOT RLIKE 'L1.*'
        AND NOT ((GROUPING_ID(R_Mg) = 0 AND R_Mg IS NULL) 
            OR (GROUPING_ID(R_Market) = 0 AND R_Market IS NULL)
            OR (GROUPING_ID(R_Country) = 0 AND R_Country IS NULL) 
            OR (GROUPING_ID(R_Subgroup) = 0 AND R_Subgroup IS NULL)
            OR (GROUPING_ID(Commercial_team) = 0 AND NULLIF(Commercial_team,'') IS NULL)
            OR (GROUPING_ID(Commercial_subteam) = 0 AND NULLIF(Commercial_subteam,'') IS NULL))

	; WITH Cte_Nmc AS (

		SELECT NMCStructure_2 AS Code, ORU_id, MRU_id, CAST(LEFT(Fiscal_Year_Period,3) AS INT) AS Period, CAST(RIGHT(Fiscal_Year_Period,4) AS INT) AS Year, 
			CASE DataType WHEN 'LYActual' THEN 'Actual' WHEN 'CYActual' THEN 'Actual' WHEN 'Target' THEN 'AOP' WHEN 'FC' THEN @ActFc END AS DataType,
			SUM(Value) AS Value 
		FROM Db.Schema.Database_H
		UNPIVOT ( Value for DataType IN (LYActual, CYActual, FC, Target) ) upv
		WHERE NMCStructure_2 IS NOT NULL AND Value <> 0
		GROUP BY NMCStructure_2, FISCAL_YEAR_PERIOD, ORU_id, MRU_id, DataType

	), Cte_Nmc_Grp AS (
		
		SELECT kpi.DeckKpi AS Kpi, DataType, Year, Period,
			COALESCE(oru.LVL03, oru.LVL04, 'L00100') AS Oru, COALESCE(mru.LVL03, mru.LVL04, 'PD0100') AS Mru, GROUPING_ID(oru.LVL03) AS grpOruL3, GROUPING_ID(oru.LVL04) AS grpOruL4, 
			SUM(Value * 1000000) AS Value
		FROM Cte_Nmc nmc
		INNER JOIN DeckReporting_Prod.Mappings.TB_Deck_Source_Kpis kpi ON nmc.Code = kpi.SourceKpi AND kpi.Source = 'NMC'
		INNER JOIN KF_DB.MD.EFR_MRU mru ON nmc.MRU_id = mru.CHILD_ID
		LEFT JOIN KF_DB.MD.EFR_ORU oru ON nmc.ORU_id = oru.H_KEY
		WHERE DataType IN ('Actual','AOP',@ActFc)
			AND NOT ((mru.CHILD_ID LIKE 'BS____' AND mru.LVL04 NOT LIKE 'BS____') OR (oru.H_KEY = 'L00100' AND oru.LVL04 <> 'L00100'))
		GROUP BY GROUPING SETS (
			(kpi.DeckKpi, DataType, Period, Year, oru.LVL03),
			(kpi.DeckKpi, DataType, Period, Year, oru.LVL04),
			(kpi.DeckKpi, DataType, Period, Year, mru.LVL03),
			(kpi.DeckKpi, DataType, Period, Year, mru.LVL03, oru.LVL03),
			(kpi.DeckKpi, DataType, Period, Year, mru.LVL03, oru.LVL04),
			(kpi.DeckKpi, DataType, Period, Year, mru.LVL04),
			(kpi.DeckKpi, DataType, Period, Year, mru.LVL04, oru.LVL03),
			(kpi.DeckKpi, DataType, Period, Year, mru.LVL04, oru.LVL04)
		)
	)

		INSERT INTO DeckReporting_Prod.DeckBase.TB_Deck_Source_Data (Sources, Kpi, Oru, Mru, DataType, Year, Period, TypePeriod, TypeOfCalc, Currency, Value)
		SELECT 'NMC' AS Sources, Kpi, Oru, Mru, DataType, Year, Period, 'ITM' AS TypePeriod, 'VAL' AS TypeOfCalc, 'EUR' AS Currency, Value
		FROM Cte_Nmc_Grp
		WHERE NOT (Oru = 'L00100' AND Mru LIKE 'B[S,U]____' AND (grpOruL3 <> 1 OR grpOruL4 <> 1))

			-------------------- Logs --------------------
			SET @insertedRows = @insertedRows + @@ROWCOUNT


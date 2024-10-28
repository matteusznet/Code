
	/*****   | DPO and DSO Calculation |   *****
		1. Cte_Src_Kpis - retreiving KPIs needed for the calculations from BPC and from TB_All_Results for LY Sales.
		2. Cte_Src_Kpis_Pvt - transforming KPIs into meassures.
		3. Cte_Src_Kpis_Days - the number of days in the month is joined to Cte_Src_Kpis_Pvt. DSO_GrossSales is calculated: DSO_GrossSales = A_Sales_3rds - DSO_Discount
		4. Cte_Dpo_Calc - final calculation of DPO
		5. Cte_Dso_Calc - calculation of substancial components for DSO. Final DSO formulae is used in the INSERT statemnt.

		SELECT * FROM DeckReporting.BPC.TB_BPC_Raw_Data_FC_Divisions WHERE PlanItem = 'NUM_DAYS'
	*****/

		; WITH Cte_Src_Kpis AS (
		
			SELECT Code, Oru, Mru, Year, Period, TypePeriod, Currency, CASE WHEN Code LIKE 'DPO%' THEN ABS(Value) ELSE Value END AS Value
			FROM #BpcData
			WHERE Code IN ('DPO_Adj_Cos','DPO_Recivables','DSO_Recivables','A_Sales_3rds','DSO_Discount') 
				AND TypePeriod IN ('ITM','ITQ') AND Mru IN ('BS9001','BS9002','BS9003','BU9669') AND Oru = 'L00100' AND Currency = 'EUR'

			UNION ALL

			-- DSO LY Sales
			SELECT 'A_Sales_3rds' AS Code, Oru, Mru, Year, Period, TypePeriod, Currency, Value
			FROM DeckReporting.CommSrc.TB_All_Results
			WHERE Code = 'MIX_ADJ_Sales_3rds'
				AND TypePeriod IN ('ITM','ITQ') AND Mru IN ('BS9001','BS9002','BS9003','BU9669')  AND Oru = 'L00100' AND Currency = 'EUR' 
				AND DataType = 'ACTUAL' AND Year = @WorkingFcYear-1

		), Cte_Src_Kpis_Pvt AS (

			SELECT Oru, Mru, Year, Period, TypePeriod, Currency, DPO_Adj_Cos, DPO_Recivables, DSO_Recivables, A_Sales_3rds, DSO_Discount
			FROM Cte_Src_Kpis
			PIVOT ( MAX(Value) FOR Code IN (DPO_Adj_Cos, DPO_Recivables, DSO_Recivables, A_Sales_3rds, DSO_Discount) ) pvt

		), Cte_Src_Kpis_Days AS (

			SELECT pv.Oru, pv.Mru, pv.Year, pv.Period, pv.TypePeriod, pv.Currency, 
				DPO_Adj_Cos, DPO_Recivables, DSO_Recivables, A_Sales_3rds - ISNULL(DSO_Discount,0) AS DSO_GrossSales, ds.Value AS DaysNo
			FROM Cte_Src_Kpis_Pvt pv
			INNER JOIN #BpcData ds ON ds.Year = pv.Year AND ds.Period = pv.Period AND ds.TypePeriod = pv.TypePeriod
			WHERE ds.Code = 'NUM_DAYS'
		
		), Cte_Dpo_Calc AS (

			SELECT Oru, Mru, Year, Period, TypePeriod, Currency,
				-- WHEN clause: Checks how many past months of DPO_Adj_Cos we have to aggregate to exceed the DSO_Recivables of the current month. THEN clause: Returns the sum of the days in the not-exceeding-months (DpoValue).
				CASE WHEN DPO_Recivables < DPO_Adj_Cos THEN 0
					WHEN (DPO_Recivables - SUM(DPO_Adj_Cos) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) < 0 THEN SUM(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 0 PRECEDING AND CURRENT ROW) 
					WHEN (DPO_Recivables - SUM(DPO_Adj_Cos) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)) < 0 THEN SUM(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
					WHEN (DPO_Recivables - SUM(DPO_Adj_Cos) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)) < 0 THEN SUM(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
					WHEN (DPO_Recivables - SUM(DPO_Adj_Cos) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) < 0 THEN SUM(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
					WHEN (DPO_Recivables - SUM(DPO_Adj_Cos) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)) < 0 THEN SUM(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
					WHEN (DPO_Recivables - SUM(DPO_Adj_Cos) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) < 0 THEN SUM(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
				END AS DpoValue
			FROM Cte_Src_Kpis_Days
			WHERE DPO_Adj_Cos IS NOT NULL

		), Cte_Dso_Calc AS (
			
			SELECT Oru, Mru, Year, Period, TypePeriod, Currency, DSO_Recivables, DSO_GrossSales, DaysNo,
				-- WHEN clause: Checks how many past months of GrossSales we have to aggregate to exceed the DSO_Recivables of the current month. THEN clause: Returns the sum of the days in the not-exceeding-months (DsoFullMonthDays).
				CASE WHEN DSO_Recivables < DSO_GrossSales THEN 0
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) < 0 THEN SUM(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 0 PRECEDING AND CURRENT ROW) 
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)) < 0 THEN SUM(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)) < 0 THEN SUM(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) < 0 THEN SUM(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)) < 0 THEN SUM(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) < 0 THEN SUM(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
				END AS DsoFullMonthDays,
				-- WHEN clause: Checks how many past months of GrossSales we have to aggregate to exceed the DSO_Recivables of the current month. THEN clause: Returns the number of the days in the exceeding-month (DsoRemainMonthDays).
				CASE WHEN DSO_Recivables < DSO_GrossSales THEN DaysNo
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) < 0 THEN MAX(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) 
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)) < 0 THEN MAX(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 2 PRECEDING AND 2 PRECEDING)
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)) < 0 THEN MAX(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 3 PRECEDING AND 3 PRECEDING)
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) < 0 THEN MAX(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 4 PRECEDING AND 4 PRECEDING)
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)) < 0 THEN MAX(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 5 PRECEDING AND 5 PRECEDING)
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) < 0 THEN MAX(DaysNo) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 6 PRECEDING AND 6 PRECEDING)
				END AS DsoRemainMonthDays,
				-- WHEN clause: Checks how many past months of GrossSales we have to aggregate to exceed the DSO_Recivables of the current month. THEN clause: Returns the value of DSO_Recivables of the current month decreased by sum of GrossSales from all not-exceeding-months (DsoRemainReciv).
				CASE WHEN DSO_Recivables < DSO_GrossSales THEN DSO_Recivables
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) < 0 THEN DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 0 PRECEDING AND CURRENT ROW) 
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)) < 0 THEN DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)) < 0 THEN DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) < 0 THEN DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)) < 0 THEN DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) < 0 THEN DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
				END AS DsoRemainReciv,
				-- WHEN clause: Checks how many past months of GrossSales we have to aggregate to exceed the DSO_Recivables of the current month. THEN clause: Returns the value of GrossSales in the exceeding-month (DsoRemainSales).
				CASE WHEN DSO_Recivables < DSO_GrossSales THEN DSO_GrossSales
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) < 0 THEN MAX(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) 
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)) < 0 THEN MAX(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 2 PRECEDING AND 2 PRECEDING) 
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)) < 0 THEN MAX(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 3 PRECEDING AND 3 PRECEDING) 
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)) < 0 THEN MAX(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 4 PRECEDING AND 4 PRECEDING) 
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)) < 0 THEN MAX(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 5 PRECEDING AND 5 PRECEDING) 
					WHEN (DSO_Recivables - SUM(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) < 0 THEN MAX(DSO_GrossSales) OVER (PARTITION BY Mru, TypePeriod ORDER BY Year ASC, Period ASC ROWS BETWEEN 6 PRECEDING AND 6 PRECEDING) 
				END AS DsoRemainSales
			FROM Cte_Src_Kpis_Days
			WHERE DSO_GrossSales IS NOT NULL
		)

			INSERT INTO #BpcData (Code, Oru, Mru, DataType, CalcName, Year, Period, TypePeriod, Currency, TypeOfCalc, Value)
			SELECT Code, Oru, Mru, @WorkingFc AS DataType, CalcName, Year, Period, TypePeriod, Currency, 'VAL' AS TypeOfCalc, Value*1000000 AS Value
			FROM (
				-- REP_DPO ITM, ITQ
				SELECT 'REP_DPO' AS Code, Oru, Mru, 'REP_DPO' AS CalcName, Year, Period, TypePeriod, Currency, DpoValue AS Value
				FROM Cte_Dpo_Calc
			UNION ALL
				-- REP_DSO ITM, ITQ
				SELECT 'Division DSO-DSO-' AS Code, Oru, Mru, 'MANUAL' AS CalcName, Year, Period, TypePeriod, Currency, DsoFullMonthDays + DsoRemainMonthDays * DsoRemainReciv / NULLIF(DsoRemainSales,0) AS Value
				FROM Cte_Dso_Calc
			) un	
			WHERE Year = @WorkingFcYear

USE Db
GO

ALTER PROCEDURE Schema.SP_Procedure_1
    @ProcStep INT, @RepYear INT, @RepMonth INT, @RepQuart INT, @WorkingFc NVARCHAR(50)
AS  

/********************************************************************************
	Last modifications:
		- 13/12/2022 - Procedure creation
	Calculated KPIS:
		- BPC FC Markets: SELECT * FROM Db.Schema.Table_2
*********************************************************************************/


DECLARE @ErrorNumber INT
DECLARE @InputMessage NVARCHAR(4000)
DECLARE @ReturnStatus INT = 0
DECLARE @StartTime DATETIME2 = GETDATE()
DECLARE @RunTimeS INT
DECLARE @RunTimeHMS NVARCHAR(10)
DECLARE @insertedRows INT = 0
DECLARE @ProcName NVARCHAR(100) = CONCAT(RIGHT(CONCAT('0',@ProcStep),2), '. ', OBJECT_NAME(@@PROCID))

BEGIN TRY
BEGIN TRANSACTION



DECLARE @BpcCsgCurrency NCHAR(11) = CASE CAST(@repYear AS INT) % 3 WHEN 0 THEN 'EUR_AOP_YR3' WHEN 1 THEN 'EUR_AOP_YR1' WHEN 2 THEN 'EUR_AOP_YR2' END
DECLARE @WorkingFcMonth INT = CASE WHEN @RepMonth = 12 THEN 1 ELSE @RepMonth+1 END
DECLARE @WorkingFcQuart INT = CASE WHEN @RepQuart = 4 THEN 1 ELSE @RepQuart+1 END
DECLARE @WorkingFcYear INT = CASE WHEN @RepQuart = 4 THEN @RepYear+1 ELSE @RepYear END

IF @RepMonth = 12 	SELECT @RepYear = @RepYear+1, @RepMonth = 1

/*****   | Load BPC import |   
	1. BPC data imported from csv file to the staging table Db.BPC.Table_2 with use of SSIS process.
	2. Db.Mappings.TB_DeckBase_BPC_Mkt_KPIs is joined in the query below to map the final names of KPIs and limit their number.
	3. Sales for CSG calculation are identified with use of the Currency column (in SELECT CASE clause)
	4. BPC ORU names are modified to be compliant with other systems - e.g. L30031 instead of P30031 (in SELECT CASE clause)
*****/

	IF OBJECT_ID(N'tempdb..#BpcData') IS NOT NULL DROP TABLE #BpcData
	CREATE TABLE #BpcData ( Currency NVARCHAR(20), Code NVARCHAR(100), Year INT, Period INT, TypePeriod NVARCHAR(3), DataType NVARCHAR(20), CalcName NVARCHAR(100),	TypeOfCalc NCHAR(3), Mru NVARCHAR(40), Oru NVARCHAR(40), Value NUMERIC(26,6) )

	INSERT INTO #BpcData (Currency, Code, Year, Period, TypePeriod, DataType, CalcName, TypeOfCalc, Mru, Oru, Value)
	SELECT Currency, Code, Year, Period, TypePeriod, DataType, Code AS CalcName, TypeOfCalc, Mru, Oru, SUM(Value) AS Value
	FROM (
		SELECT CASE WHEN Currency = @BpcCsgCurrency THEN 'EUR' ELSE Currency END AS Currency, 
			'BPC_' + @WorkingFc AS DataType, 'VAL' AS TypeOfCalc,
			CASE WHEN Currency = @BpcCsgCurrency AND LEFT(Time,4) = @RepYear THEN 'SALES_CSG_COMP_CY'
				WHEN Currency = @BpcCsgCurrency AND LEFT(Time,4) = @RepYear-1 THEN 'SALES_CSG_COMP_LY'
				ELSE kpi.DecksKpi 
			END AS Code,
			CASE WHEN RIGHT(Time,5) = 'TOTAL' THEN 12
				WHEN RIGHT(Time,2) LIKE 'Q[1-4]' THEN CAST(RIGHT(Time,1) AS INT)
				WHEN RIGHT(Time,2) LIKE '[0-1][0-9]' THEN CAST(RIGHT(Time,2) AS INT)
			END AS Period,
			CASE WHEN RIGHT(Time,5) = 'TOTAL' THEN 'FY'
				WHEN RIGHT(Time,2) LIKE 'Q[1-4]' THEN 'ITQ'
				WHEN RIGHT(Time,2) LIKE '[0-1][0-9]' THEN 'ITM'
			END AS TypePeriod,
			CASE Currency WHEN @BpcCsgCurrency THEN @RepYear ELSE LEFT(Time,4) END AS Year,
			CASE Business WHEN 'DLS001' THEN 'BS9001' 
				 WHEN 'DLS004' THEN 'BS9003' 
				 WHEN 'DLS005' THEN 'BS9002' 
				 ELSE Business 
			END AS Mru,
			CASE WHEN Oru = 'P20004' THEN 'L30023'
				 WHEN Oru = 'P40005' AND BpcKpi = '192410_BA' THEN 'L30011'
			 	 WHEN Oru LIKE 'P[2-3]%' THEN REPLACE(Oru,'P','L') 
				 ELSE REPLACE(Oru,'P','') 
			END AS Oru,
			Amount AS Value
		FROM Db.Schema.Table_2 bpc
		LEFT JOIN Db.Mappings.TB_BPC_Mkt_KPIs kpi
			ON bpc.PlanItem = kpi.BpcKpi
		WHERE (kpi.BpcKpi IS NOT NULL OR Currency = @BpcCsgCurrency)
			AND (Currency IN ('EUR','USD',@BpcCsgCurrency) OR (Currency = 'LC' AND PlanItem = 'NUM_DAYS'))
			AND NOT (Oru = 'P30011' AND BpcKpi = '192410_BA')
	) AS sl
	GROUP BY Currency, Code, Year, Period, TypePeriod , DataType, TypeOfCalc, Mru, Oru




/*****   | YTQ, YTD calcualtion based on ITQ |   *****/

	INSERT INTO #BpcData (Currency, Code, Year, Period, TypePeriod, DataType, TypeOfCalc, Mru, Oru, Value)
		-- YTQ calculation
		SELECT Currency, Code, Year, 0 AS Period, 'YTQ' AS TypePeriod, DataType, TypeOfCalc, Mru, Oru, SUM(Value) AS Value
		FROM #BpcData 
		WHERE Period <= @RepQuart AND TypePeriod = 'ITQ'
		GROUP BY Currency, Code, Year, DataType, TypeOfCalc, Mru, Oru
	UNION ALL
		-- YTD calcualtion
		SELECT Currency, Code, Year, 0 AS Period, 'YTD' AS TypePeriod, DataType, TypeOfCalc, Mru, Oru, SUM(Value) AS Value
		FROM #BpcData 
		WHERE Period <= @RepMonth AND TypePeriod = 'ITM'
		GROUP BY Currency, Code, Year, DataType, TypeOfCalc, Mru, Oru




/*****   | ORU and MRU Exceptions |   *****/

	-- | Pierlite exception | -- New ORUs assigned by joining with #PierliteMap mapping. Multiplier column defines if ORUs are included or excluded
	IF OBJECT_ID(N'tempdb..#PierliteMap') IS NOT NULL DROP TABLE #PierliteMap
		SELECT IniOru = '152006', FinOru = 'PIERLIGHT', 		Multiplier = 1		INTO #PierliteMap UNION ALL
		SELECT IniOru = '702001', FinOru = 'PIERLIGHT', 		Multiplier = 1		UNION ALL
		SELECT IniOru = 'L30032', FinOru = 'L30032_EXCL_PIER', 	Multiplier = 1		UNION ALL
		SELECT IniOru = '152006', FinOru = 'L30032_EXCL_PIER', 	Multiplier = -1		UNION ALL
		SELECT IniOru = '702001', FinOru = 'L30032_EXCL_PIER', 	Multiplier = -1

	INSERT INTO #BpcData (Currency, Code, Year, Period, TypePeriod, DataType, TypeOfCalc, Mru, Oru, Value)
	SELECT Currency, Code, Year, Period, TypePeriod, DataType, TypeOfCalc, Mru, prl.FinOru, SUM(Multiplier*Value) AS Value
	FROM #BpcData bpc
	INNER JOIN #PierliteMap prl
		ON bpc.Oru = prl.IniOru
	GROUP BY Currency, Code, Year, Period, TypePeriod, DataType, TypeOfCalc, Mru, prl.FinOru



	/**  ITQ, YTQ, FY Calculation for KPIs with exceptions  **/

	; WITH Cte_Itq_Wfc AS (

		-- ITQ BPC Working FC for future months
		SELECT Currency, Code, Year, Period, TypePeriod, DataType, TypeOfCalc, Mru, Oru, Value
		FROM #BpcData 
		WHERE ( 	Code IN ('ADJ_ISM_Market','VIPP_ADJ_IGM_Total','ADJ_Sellex_Market','ADJ_Sellex_A&P','ADJ_Sellex_OFSE_Local','ADJ_Sellex_TPW')
				OR (Code IN ('Adjustments_Restructuring','REP_WC') AND Mru = 'PD0100')			) 
		AND TypePeriod = 'ITQ' AND NOT (TypePeriod = 'ITQ' AND Year = @WorkingFcYear AND Period < @WorkingFcQuart)
		AND MRU NOT LIKE 'LI[_]%'

			UNION ALL

		-- ITQ Actuals replacing closed months of Working FC 
		SELECT Currency, 
			CASE WHEN Code = 'VIPP Adjusted IGM' THEN 'VIPP_ADJ_IGM_Total' ELSE Code END AS Code,
			Year, Period, TypePeriod, 'BPC_' + @WorkingFc AS DataType, 'VAL' AS TypeOfCalc, Mru, Oru, Value
		FROM Db.Schema.Table_1
		WHERE (		Code IN ('ADJ_ISM_Market','VIPP Adjusted IGM','ADJ_Sellex_Market','ADJ_Sellex_A&P','ADJ_Sellex_OFSE_Local','ADJ_Sellex_TPW')
				OR (Code IN ('Adjustments_Restructuring','REP_WC')	AND Mru = 'PD0100')			)
		AND TypePeriod = 'ITQ' AND DataType = 'ACTUAL' AND Year = @WorkingFcYear AND Period < @WorkingFcQuart	

	), Cte_Ytq_Fy AS (

		-- Calculation of YTQ AND FY basing on previously modified ITQ
		SELECT Currency, Code, Year, Period, TypePeriod, DataType, TypeOfCalc, Mru, Oru, SUM(Value) AS Value 
		FROM (
			SELECT Currency, Code, Year, tp.TypePeriod, DataType, TypeOfCalc, Mru, Oru, Value,
				CASE tp.TypePeriod WHEN 'ITQ' THEN Period WHEN 'YTQ' THEN 0 WHEN 'FY' THEN 12 END AS Period
			FROM Cte_Itq_Wfc cte
			CROSS JOIN (SELECT 'ITQ' AS TypePeriod UNION ALL SELECT 'YTQ' AS TypePeriod UNION ALL SELECT 'FY' AS TypePeriod) tp
			WHERE NOT (tp.TypePeriod = 'YTQ' AND Period > @RepQuart)
		) grp
		GROUP BY Currency, Code, Year, Period, TypePeriod, DataType, TypeOfCalc, Mru, Oru

	), Cte_Merge_Tgt AS (

		SELECT Currency, Code, Year, Period, TypePeriod, DataType, TypeOfCalc, Mru, Oru, Value
		FROM #BpcData
		WHERE Code IN (SELECT DISTINCT Code FROM Cte_Itq_Wfc)
			AND TypePeriod IN ('ITQ','YTQ','FY')
			AND MRU NOT LIKE 'LI[_]%'
	)

		MERGE Cte_Merge_Tgt AS TARGET
		USING Cte_Ytq_Fy AS SOURCE
			ON TARGET.Currency = SOURCE.Currency AND TARGET.Code = SOURCE.Code AND TARGET.Year = SOURCE.Year AND TARGET.Period = SOURCE.Period AND TARGET.TypePeriod = SOURCE.TypePeriod 
			AND TARGET.DataType = SOURCE.DataType AND TARGET.TypeOfCalc = SOURCE.TypeOfCalc AND TARGET.Mru = SOURCE.Mru AND TARGET.Oru = SOURCE.Oru
		WHEN MATCHED THEN UPDATE SET 
			TARGET.Value = SOURCE.Value
		WHEN NOT MATCHED BY TARGET THEN
			INSERT (Currency, Code, Year, Period, TypePeriod, DataType, TypeOfCalc, Mru, Oru, Value)
			VALUES (SOURCE.Currency, SOURCE.Code, SOURCE.Year, SOURCE.Period, SOURCE.TypePeriod, SOURCE.DataType, SOURCE.TypeOfCalc, SOURCE.Mru, SOURCE.Oru, SOURCE.Value)
		WHEN NOT MATCHED BY SOURCE THEN
			DELETE ;



        /***   Insert to TB_All_Results   ***/

        DELETE FROM Db.Schema.Table_1 WHERE Sources = 'BPC_MGMK'

        INSERT INTO DeckReporting.CommSrc.TB_All_Results (Sources, Code, Oru, Mru, DataType, Year, Period, TypePeriod, Currency, TypeOfCalc, CalcName, Value)
        SELECT 'BPC_MGMK' AS Sources, Code, Oru, Mru, DataType, Year, Period, TypePeriod, Currency, TypeOfCalc,
            CASE WHEN CalcName IS NULL THEN Code ELSE CalcName END AS CalcName, Value 
        FROM #BpcData 
        WHERE NOT (Oru LIKE 'L2000_' AND Mru LIKE 'B[S,U]____')

            -------------------- Logs --------------------
            SET @insertedRows = @insertedRows + @@ROWCOUNT




/*** Procedure end statements ***/

	SET @ReturnStatus = 1;
    SET @RunTimeS = (SELECT DATEDIFF(s,@StartTime,GETDATE()))
    SET @RunTimeHMS = CONCAT(@RunTimeS/3600,':',RIGHT(CONCAT('0',(@RunTimeS%3600)/60),2),':',RIGHT(CONCAT('0',(@RunTimeS%3600)%60),2))
    SET @InputMessage = CONCAT(@ProcName, ': Process Completed. | Inserted Rows: ', @insertedRows, ' | Elapsed time: ', @RunTimeHMS)
    EXEC Db.dbo.sLogging @Category = 'MprDataLoad',  @LogText = @InputMessage;

COMMIT TRANSACTION
	SET @ReturnStatus = 1;

END TRY
BEGIN CATCH
    DECLARE @xstate INT = XACT_STATE()
	IF @xstate != 0 ROLLBACK TRANSACTION
   
    SET @ErrorNumber = ERROR_NUMBER();
    SET @InputMessage = OBJECT_NAME(@@PROCID) + N': ' + ERROR_MESSAGE() + N', Error Line: ' + CAST(ERROR_LINE() AS nvarchar(10));
    EXEC Db.dbo.sLogging @Category = 'MprDataLoad', @LogText = @InputMessage, @ErrorNumber = @ErrorNumber; 
END CATCH

RETURN(@ReturnStatus)

GO
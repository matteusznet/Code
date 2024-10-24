
USE DeckReporting_Prod 
GO

ALTER PROCEDURE SourceData.SP_Deck_Load_Csg (
	@ProcStep AS INT, @RepYear AS INT, @RepMonth INT, @RepQuart INT, @ActFc AS VARCHAR(50)
) AS

	/********************************************************************************
		Last modifications:
			- 01/10/2022 - Procedure creation
		Calculated KPIS:
			- CSG 
			- SELECT DISTINCT 'CSG-' + BU_Technology AS Code FROM Csg_Base3.Perc.Csg_Data_Table WHERE System = 'VIPP_BU' AND Mru_Id_Key IN ('BU4032','BU4035','BU0118','BU9174','BU9540')
			- FC_ACCU_HISTORICAL, FC_ACCU_HISTORICAL_RANK
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


		/*****   CSG   *****/ 

		; WITH Cte_Csg_Src AS (

			-- Standard CSG
			SELECT 'CSG' AS Code, Oru_Id_Key, Mru_Id_Key, Query, Type_Data, [001], [002], [003], [004], [005], [006], [007], [008], [009], [010], [011], [012], [FY] 
			FROM Db.Schema.Table_1
			WHERE System = 'EFR' AND Select_Led = 'TOTAL' AND (Bmc_Select = 'BMC' OR Bmc_Select = 'TOTAL')

				UNION ALL 

			-- CSG with Technology split
			SELECT 'CSG-' + BU_Technology AS Code, Oru_Id_Key, Mru_Id_Key, Query, Type_Data, [001], [002], [003], [004], [005], [006], [007], [008], [009], [010], [011], [012], [FY] 
			FROM Db.Schema.Table_1
			WHERE System = 'VIPP_BU' AND Mru_Id_Key IN ('BU4032','BU4035','BU0118','BU9174','BU9540')

		), Cte_Csg_Upv AS (

			-- Unpivot month numer measures into a single Period column and a Value column
			SELECT Code, Oru_Id_Key AS Oru,
				CASE WHEN Mru_Id_Key = 'Agriculture' AND Oru_Id_Key = 'L00100' THEN 'AGRI_incl_Flu' WHEN Mru_Id_Key IN ('Agriculture','Agri_Excl_Fluence') THEN 'AGRI' ELSE Mru_Id_Key END AS Mru,
				CASE Type_Data WHEN CONCAT('A',@RepYear) THEN 'AOP' WHEN 'FC'+SUBSTRING(@ActFc,10,4)+SUBSTRING(@ActFc,6,3) THEN @ActFc ELSE 'Actual' END AS DataType,
				CASE Period WHEN 'FY' THEN 'FY' ELSE Query END AS TypePeriod,
				CASE WHEN Type_Data LIKE 'FC%' THEN SUBSTRING(@ActFc,10,4) ELSE RIGHT(Type_Data,4) END AS Year,
				CASE WHEN Period = 'FY' THEN 12 
					WHEN Query = 'ITQ' AND Period = '003' THEN 1 
					WHEN Query = 'ITQ' AND Period = '006' THEN 2
					WHEN Query = 'ITQ' AND Period = '009' THEN 3
					WHEN Query = 'ITQ' AND Period = '012' THEN 4
					ELSE CAST(Period AS INT) 
				END AS Period,
				Value
			FROM Cte_Csg_Src
			UNPIVOT ( Value FOR Period IN ([001],[002],[003],[004],[005],[006],[007],[008],[009],[010],[011],[012],[FY]) ) upv
			WHERE Type_Data IN ( CONCAT('A',@RepYear), CONCAT('Actual',@RepYear-1), CONCAT('Actual',@RepYear), 'FC'+SUBSTRING(@ActFc,10,4)+SUBSTRING(@ActFc,6,3) )
				AND ( Query IN ('ITM','YTD') OR (Query = 'ITQ' AND Period IN ('003','006','009','012')) )
				AND ( Period <> 'FY' OR (Period = 'FY' AND Query = 'YTD') )

		)

			INSERT INTO DeckReporting_Prod.DeckBase.TB_Deck_Source_Data (Sources, Kpi, Oru, Mru, DataType, Year, Period, TypePeriod, TypeOfCalc, Currency, Value)
				-- ITM, ITQ, YTD, FY
				SELECT '4Years' AS Sources, Code, Oru, Mru, DataType, Year, Period, TypePeriod, 'PER' AS TypeOfCalc, 'EUR' AS Currency, Value
				FROM Cte_Csg_Upv
			UNION ALL
				-- YTQ
				SELECT '4Years' AS Sources, Code, Oru, Mru, DataType, Year, 0 AS Period, 'YTQ' AS TypePeriod, 'PER' AS TypeOfCalc, 'EUR' AS Currency, Value
				FROM Cte_Csg_Upv
				WHERE TypePeriod = 'YTD' AND ( (DataType = 'Actual' AND Period = @RepMonth) OR Period = 3*@RepQuart )

				-------------------- Logs --------------------
				SET @insertedRows = @insertedRows + @@ROWCOUNT



		/*****   | Forecast Accuracy |   *****/

		; WITH Cte_Csg_Acc AS (

			SELECT Oru_Level4 AS Oru, Mru_L3_Bg_Key AS Mru, Query AS TypePeriod,
				CASE WHEN Type_Data LIKE 'FC%001' THEN 'FCQ1' WHEN Type_Data LIKE 'FC%004' THEN 'FCQ2' WHEN Type_Data LIKE 'FC%007' THEN 'FCQ3' WHEN Type_Data LIKE 'FC%010' THEN 'FCQ4' ELSE 'Actual' END AS DataType,
				CASE WHEN Type_Data LIKE 'FC%' THEN SUBSTRING(Type_Data,3,4) ELSE RIGHT(Type_Data,4) END AS Year,
				CASE RIGHT(Period,2) WHEN '03' THEN 1 WHEN '06' THEN 2 WHEN '09' THEN 3 WHEN '12' THEN 4 ELSE CAST(RIGHT(Period,2) AS INT) END AS Period, 
				SUM(Value) AS Value
			FROM Db.Schema.Table_2
			UNPIVOT ( Value FOR Period IN (Comp_CY_01, Comp_CY_02, Comp_CY_03, Comp_CY_04, Comp_CY_05, Comp_CY_06, Comp_CY_07, Comp_CY_08, Comp_CY_09, Comp_CY_10, Comp_CY_11, Comp_CY_12) ) upv
			WHERE BMC_Select = 'BMC' AND System = 'EFR' AND Csg_Bu = 'CSG'
				AND ( Type_Data LIKE 'Actual____' OR LEFT(Type_Data,6) IN ( CONCAT('FC',@RepYear), CONCAT('FC',@RepYear-1) ) OR Type_Data = CONCAT('FC',@RepYear-2,'010') )
				AND Query = 'ITQ' AND RIGHT(Period,2) IN ('03','06','09','12')
				AND Mru_L3_Bg_Key LIKE 'BS%'
			GROUP BY ORU_Level4, Mru_L3_Bg_Key, Type_Data, Query, Period

		), Cte_Acc_Hist AS (

			-- Window function LAG used instead unpivot to get Act and Fc as 2 separate column
			-- Calculations SUM(ABS(Act - Fc))/SUM(Fc) performed in the outer query
			SELECT TypePeriod, Oru, 'PD0100' AS Mru, DataType, Year, Period, 1 - SUM(ABS(Act - Fc))/SUM(Fc) AS Value
			FROM (
				SELECT TypePeriod, Oru, Mru, DataType, Year, Period, Value AS Act, 
					LAG(Value,1) OVER (PARTITION BY Oru, Mru, Year, Period ORDER BY DataType DESC) AS Fc
				FROM Cte_Csg_Acc
				WHERE ( DataType = 'Actual' OR (DataType LIKE 'FC%' AND CAST(RIGHT(DataType,1) AS INT) = Period) )
			) wfc
			WHERE Fc IS NOT NULL
			GROUP BY TypePeriod, Oru, DataType, Year, Period

		)

		INSERT INTO DeckReporting_Prod.DeckBase.TB_Deck_Source_Data (Sources, Kpi, Oru, Mru, DataType, CalcName, Year, Period, TypePeriod, TypeOfCalc, Currency, Value)
			-- FC_ACCU_HISTORICAL
			SELECT '4Years' AS Sources, 'FC_ACCU_HISTORICAL' AS Code, Oru, Mru, 'NONE' AS DataType, 'MANUAL' AS CalcName, Year, Period, TypePeriod, 'PER' AS TypeOfCalc, 'NONE' AS Currency,
				Value
			FROM Cte_Acc_Hist
		UNION ALL
			-- FC_ACCU_HISTORICAL_RANK - RANK window function used
			SELECT '4Years' AS Sources, 'FC_ACCU_HISTORICAL_RANK' AS Code, CASE WHEN Oru = 'L30023' THEN 'L20004' ELSE Oru END AS Oru, Mru, 'NONE' AS DataType, 
				'MANUAL' AS CalcName, Year, Period, TypePeriod, 'RANK' AS TypeOfCalc, 'NONE' AS Currency,
				RANK() OVER (PARTITION BY Year, Period ORDER BY Value DESC) AS Value
			FROM Cte_Acc_Hist





	/*** Procedure end statements ***/

		SET @ReturnStatus = 1;
		SET @RunTimeS = (SELECT DATEDIFF(s,@StartTime,GETDATE()))
		SET @RunTimeHMS = CONCAT(@RunTimeS/3600,':',RIGHT(CONCAT('0',(@RunTimeS%3600)/60),2),':',RIGHT(CONCAT('0',(@RunTimeS%3600)%60),2))
		SET @InputMessage = CONCAT(@ProcName, ': Process Completed. | Inserted Rows: ', @insertedRows, ' | Elapsed time: ', @RunTimeHMS)
		EXEC DeckReporting_Prod.Process.SP_Deck_Refresh_Logging @Category = 'MprDataLoad',  @LogText = @InputMessage;

	COMMIT TRANSACTION
		SET @ReturnStatus = 1;

	END TRY
	BEGIN CATCH
		DECLARE @xstate INT = XACT_STATE()
		IF @xstate != 0 ROLLBACK TRANSACTION
	
		SET @ErrorNumber = ERROR_NUMBER();
		SET @InputMessage = OBJECT_NAME(@@PROCID) + N': ' + ERROR_MESSAGE() + N', Error Line: ' + CAST(ERROR_LINE() AS nvarchar(10));
		EXEC DeckReporting_Prod.Process.SP_Deck_Refresh_Logging @Category = 'MprDataLoad', @LogText = @InputMessage, @ErrorNumber = @ErrorNumber; 
	END CATCH

	RETURN(@ReturnStatus)

GO

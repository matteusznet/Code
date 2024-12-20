

ALTER PROCEDURE Wtm_Masters.[SP_Refresh_Master_Database] AS

	TRUNCATE TABLE DB_Constructo.Wtm_Masters.T_Master_Database

	-- Cursor that runs through all the tables starting with 'T_Master_Excel_Przypisanie_' to insert their content into T_Master_Excel_Przypisanie
	DECLARE @TableNamePattern nvarchar(255)
	DECLARE @SQL nvarchar(max)
	DECLARE @tableName nvarchar(255)

	SET @TableNamePattern = 'T_Master_Master_'

	DECLARE myCursor CURSOR FOR
		SELECT name FROM Db.sys.tables
		WHERE name LIKE @TableNamePattern + '%'

	OPEN myCursor;
	FETCH NEXT FROM myCursor INTO @tableName;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @SQL = '
			INSERT INTO [DB_Constructo].[Wtm_Masters].[T_Master_Database] (Budowa, Charakterystyka, Kwota_Netto)
			SELECT
				''' + REPLACE(@tableName,@TableNamePattern,'') + ''' AS Budowa,
				CASE WHEN [Unnamed: 3] LIKE ''WARTOŚĆ KONTRAKTU NETTO%'' THEN ''Wartość kontraktu''
					 WHEN [Unnamed: 3] = ''PONIESIONY KOSZT (WSZYSTKIE FAKTURY)'' THEN ''Poniesiony koszt''
					 WHEN [Unnamed: 3] LIKE ''K.O.%'' THEN ''Koszty ogólne''
					 WHEN [Unnamed: 3] = ''BUDŻET'' THEN ''Budżet''
					 WHEN [Unnamed: 3] = ''PRZEWIDYWANY ZYSK'' THEN ''Przewidywany zysk''
				END AS Charakterystyka,
				[Unnamed: 4] AS Kwota_Netto
			FROM [Db].[Schema].[' + @tableName + ']
			WHERE [Unnamed: 3] LIKE ''WARTOŚĆ KONTRAKTU NETTO%''
				OR [Unnamed: 3] LIKE ''K.O.%''
				OR [Unnamed: 3] IN (''PONIESIONY KOSZT (WSZYSTKIE FAKTURY)'',''BUDŻET'',''PRZEWIDYWANY ZYSK'')
		'
		EXEC(@SQL)

		FETCH NEXT FROM myCursor INTO @tableName;
	END;

	CLOSE myCursor;
	DEALLOCATE myCursor;

GO
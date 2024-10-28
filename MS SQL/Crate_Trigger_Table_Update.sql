CREATE TRIGGER [Process].[TR_Run_Full_Decks_Data_Load]
    ON [Db].[Process].[TB_Deck_Refresh_History_Trgr]
    AFTER UPDATE 
AS 
BEGIN

    -- Triggers the Deck Database refresh if:
    -- 1) Last set ProcessEndTime concerns 'KF Database' - Decks should start refreshing once KF is finalized
    -- 2) IsActive Flag is set to 1 in TB_Refresh_Flags - the flag enables easy management of the trigger
    -- 3) Deck Database is not already refreshing in the background - to not interrupt the proces which had been manually started

    IF (SELECT TOP(1) ProcessName FROM Db.Process.TB_Deck_Refresh_History_Trgr ORDER BY ProcessEndTime DESC) = 'KF Database'
        AND (SELECT IsActive FROM Db.Process.TB_Refresh_Flags WHERE ProcessName = 'Refresh Deck SQL Db') = 1
        AND CHARINDEX('end',(SELECT TOP(1) LogText FROM Db.Process.SystemLog NOLOCK WHERE Category LIKE 'MprDataLoad%' ORDER BY LogDate DESC, LogID DESC)) > 0
    EXEC Db.DataLoad.SP_Full_Decks_Data_Load_With_Logging

END
GO
ALTER TABLE [Process].[TB_Deck_Refresh_History_Trgr] ENABLE TRIGGER [TR_Run_Full_Decks_Data_Load]
GO

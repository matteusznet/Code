GO
CREATE OR ALTER PROCEDURE [dbo].[sp_mi_populate_eligibility] 
(
    @id_scope int = -1,
    @idMeritProcess int = -1,
    @id_user INT = -1
) AS
/**
# ===============================================================
Description: 
    The procedure populates MI Eligibility grid with data from employee situation table and MI Process Definition grid.
    Only processes exisiting in MI Process Definition grid with the status different than ARCHIVE should be updated in MI Eligibility.
    Archived processes are not deleted from MI Eligibility grid though.
Called by: -
# ===============================================================
Changes:
    - Date: 2022-02-03
    Author: Mateusz Kurzyk
    Change: Creation
    - Date: 2022-04-01
    Author: Mateusz Kurzyk
    Change: Add Function Code and Function Name columns popluation
# ===============================================================
**/
BEGIN
    DECLARE @Category NVARCHAR(255) = 'MI';
    DECLARE @Process NVARCHAR(255) = 'MI Eligibility population';
    DECLARE @SubProcess NVARCHAR(255) = OBJECT_NAME(@@PROCID);
    DECLARE @StartDate DATETIME = GETDATE();
    DECLARE @UserId INT = @id_user
    DECLARE @EventText NVARCHAR(255)
    DECLARE @AuditId INT = -1
    DECLARE @mergeAct TABLE (MrgAction nvarchar(20), ModifiedCode nvarchar(50))
    DECLARE @ins nvarchar(12) 
    DECLARE @upd nvarchar(12)
    DECLARE @del nvarchar(12)

    BEGIN TRY
        BEGIN TRANSACTION

        /**** Logs ****/
        EXEC [sp_audit_log]
            @Category = @Category,
            @Process  = @Process,
            @SubProcess = @SubProcess,
            @StartDate = @StartDate,
            @EventText = 'Procedure start',
            @AuditId = -1,
            @UserId = @UserId


        /** Employee data merge **/
        ;
        WITH cte_emplAtEligibCutOff AS ( 

            /** Get the list of employees belonging to the scope defined in MI Process Definition which are valid at the eligibility cutoff date. **/
            SELECT DISTINCT miPd.idMeritProcess, miPd.id_scope, tbrms.scope_code, miPd.eligibility_cutoff_date, miPd.employee_data_cutoff, emSi.idPayee, 
                emSi.EmployeeCode, emSi.EmployeeFirstName, emSi.EmployeeLastName, emSi.StatusEmployeeCode, emSi.EmployeeStatusStartDate, 
                emSi.EmployeeStatusEndDate, emSi.DateTermination, lcts.Country_ID, edrsj.IdStandardJob, emSi.StandardJobCode,
                tepr.idPerformanceRating, trpr.PerformanceCode, tetr.IdTalentRating, trtr.TalentCode, miPd.id_comp_round
            FROM dbo.tb_mi_process_definition miPd
            INNER JOIN _tb_bqm_ref_MI_Scope tbrms
                ON miPd.id_scope = tbrms.scope_id
            INNER JOIN dbo._tb_bqm_Link_Company_to_Scope lcts
                ON miPd.id_scope = lcts.Scope_ID
            INNER JOIN dbo._ep_dm_ref_company rfCo
                ON lcts.Company_ID = rfCo.Company_ID
            INNER JOIN dbo.tb_mi_employee_situation emSi
                ON rfCo.Company_Code = emSi.CompanyCode
            LEFT JOIN tb_employee_performance_rating tepr
                ON emSi.EmployeeCode = tepr.EmployeeCode
                AND miPd.Year = tepr.Year
            LEFT JOIN tb_ref_performance_rating trpr
                ON tepr.idPerformanceRating = trpr.idPerformanceRating
            LEFT JOIN tb_employee_talent_rating tetr
                ON emSi.EmployeeCode = tetr.EmployeeCode
                AND miPd.Year = tetr.Year
            LEFT JOIN tb_ref_talent_ratings trtr
                ON tetr.IdTalentRating = trtr.IdTalentRating
            LEFT JOIN _ep_dm_ref_standard_job edrsj
                ON emSi.StandardJobCode = edrsj.StandardJobCode
            WHERE miPd.id_status <> 3                                                    -- Merit process status is different than ARCHIVE
            AND emSi.StatusEmployeeCode IS NOT NULL
            AND miPd.eligibility_cutoff_date BETWEEN emSi.StartDate AND emSi.EndDate
            AND ( emSi.DateTermination >= miPd.eligibility_cutoff_date OR emSi.DateTermination IS NULL )
            AND ( miPd.id_scope = @id_scope OR @id_scope = -1 )
            AND ( miPd.idMeritProcess = @idMeritProcess OR @idMeritProcess = -1 )
        ),

        eligib_source AS (

            /** Combine employees at eligiblity cutoff date with their properties at employee cutoff date defined in MI Process Definition **/
            SELECT elgCut.id_scope, elgCut.scope_code, elgCut.idMeritProcess, elgCut.eligibility_cutoff_date, elgCut.employee_data_cutoff, elgCut.idPayee, elgCut.EmployeeCode,
                   elgCut.EmployeeFirstName, elgCut.EmployeeLastName, elgCut.StatusEmployeeCode, elgCut.EmployeeStatusStartDate, elgCut.EmployeeStatusEndDate, 
                   elgCut.DateTermination, empCut.OrganizationName, ISNULL(empCut.CompanyCountryCode,'') AS CompanyCountryCode, empCut.Company_ID, empCut.HireDate, empCut.LineManager1EmployeeCode, 
                   empCut.LineManager2EmployeeCode, empCut.EmployeeGroupName, empCut.EmployeeSubGroupName, empCut.CostCenterName, empCut.PayrollAreaName, 
                   empCut.PersonnelAreaName, empCut.PersonnelAreaCode, empCut.PersonnelSubAreaName, empCut.PersonnelSubAreaCode, empCut.PositionName, empCut.PositionCode, 
				   empCut.IdPositionGrade, empCut.PositionGradeCode, empCut.IdPersonalGrade, empCut.PersonalGradeCode, empCut.Age, empCut.GenderName, empCut.WorkPercentage, 
				   empCut.CurrencySalary, empCut.AnnualSalary, empCut.MonthlySalary, empCut.PreviousIncreaseDate, elgCut.Country_ID, elgCut.IdStandardJob, elgCut.StandardJobCode, 
				   empCut.CompanyCode, elgCut.idPerformanceRating, elgCut.PerformanceCode, elgCut.IdTalentRating, elgCut.TalentCode, elgCut.id_comp_round, 
				   empCut.FunctionCode, empCut.FunctionName, ISNULL(empCut.NbOfMonths, 12) AS NbOfMonths
                   --,CASE WHEN elgCut.StatusEmployeeCode = 0 THEN 1 END AS HiddenFlag
            FROM cte_emplAtEligibCutOff elgCut
            LEFT JOIN (
                SELECT miPd.id_scope, miPd.idMeritProcess, emSi.idPayee, emSi.EmployeeCode, emSi.OrganizationName, emSi.CompanyCountryCode, rfCo.Company_ID, 
                    emSi.HireDate, emSi.LineManager1EmployeeCode, emSi.LineManager2EmployeeCode, emSi.EmployeeGroupName, emSi.EmployeeSubGroupName, emSi.CostCenterName, 
                    emSi.PayrollAreaName, emSi.PersonnelAreaCode, emSi.PersonnelAreaName, emSi.PersonnelSubAreaCode, emSi.PersonnelSubAreaName, emSi.PositionCode, emSi.PositionName,
					posGr.IdPositionGrade AS IdPositionGrade, posGr.PositionGrade AS PositionGradeCode, perGr.IdPositionGrade AS IdPersonalGrade, perGr.PersonalGradeCode, 
					emSi.Age, emSi.GenderName, emSi.WorkPercentage, emSi.CurrencySalary, emSi.AnnualSalary, emSi.MonthlySalary, emSi.PreviousIncreaseDate, emSi.CompanyCode, emSi.NbOfMonths,
					emSi.FunctionCode, emSi.FunctionName
                FROM dbo.tb_mi_process_definition miPd
                INNER JOIN dbo._tb_bqm_Link_Company_to_Scope lcts
                    ON miPd.id_scope = lcts.Scope_ID
                INNER JOIN dbo._ep_dm_ref_company rfCo
                    ON lcts.Company_ID = rfCo.Company_ID
                INNER JOIN dbo.tb_mi_employee_situation emSi
                    ON rfCo.Company_Code = emSi.CompanyCode
                LEFT JOIN dbo._ep_dm_ref_positionGrade posGr
                    ON emSi.PositionGrade = posGr.PositionCode
                LEFT JOIN dbo._ep_dm_ref_personalGrade perGr
                    ON emSi.CorporateFunctionCode = perGr.PersonalGradeCode
                WHERE miPd.id_status <> 3                                                    -- Merit process status is different than ARCHIVE
                AND miPd.employee_data_cutoff BETWEEN emSi.StartDate AND emSi.EndDate
            ) empCut
            ON elgCut.EmployeeCode = empCut.EmployeeCode
            AND elgCut.idMeritProcess = empCut.idMeritProcess

        ),

        eligib_target AS ( 
            SELECT elg.IdScope, elg.ScopeCode, elg.IdMeritProcess, elg.EligibilityCutoffDate, elg.EmployeeDataCutoffDate, elg.idPayee, elg.EmployeeCode, 
                   elg.FirstName, elg.LastName, elg.EmployeeStatusAtEligibilityCutoffDate, elg.EmployeeStatusStartDate, elg.EmployeeStatusEndDate, 
                   elg.TerminationDate, elg.OrganisationalUnit, elg.IdCountry, elg.CountryCompanyCode, elg.IdCompanyCode, elg.CompanyCode, elg.EntryInCompany, elg.LineManager, 
                   elg.SecondLineManager, elg.EmployeeGroup, elg.EmployeeSubGroup, elg.CostCenter, elg.PayrollArea, 
                   elg.PersonnelAreaName, elg.PersonnelAreaCode, elg.PersonnelSubAreaName, elg.PersonnelSubAreaCode, elg.PositionName, elg.PositionCode, 
				   elg.IdPositionGrade, elg.PositionGradeCode, elg.IdPersonalGrade, elg.PersonalGradeCode, elg.Age, elg.Gender, elg.WorkPercentage, 
				   elg.Currency, elg.AnnualBaseSalary, elg.MonthlyBaseSalary, elg.PreviousIncreaseDate, elg.IdStandardJob, elg.StandardJobCode,
                   elg.idPerformanceRating, elg.PerformanceCode, elg.IdTalentRating, elg.TalentCode, elg.IdCompRound, 
				   elg.FunctionCode, elg.FunctionName, elg.NbOfMonths
                   --,elg.HiddenFlag
            FROM dbo.tb_mi_eligibility elg
            INNER JOIN dbo.tb_mi_process_definition miPd
                ON elg.IdMeritProcess = miPd.IdMeritProcess
                AND ( miPd.idMeritProcess = @idMeritProcess OR @idMeritProcess = -1 )
                AND miPd.id_status <> 3                                                        -- Merit process status is different than ARCHIVE
        )

            MERGE eligib_target AS TARGET
            USING eligib_source AS SOURCE
                ON TARGET.idMeritProcess = SOURCE.idMeritProcess
                AND TARGET.EmployeeCode = SOURCE.EmployeeCode

            WHEN MATCHED THEN UPDATE SET 
                TARGET.EligibilityCutoffDate = SOURCE.eligibility_cutoff_date, 
                TARGET.EmployeeDataCutoffDate = SOURCE.employee_data_cutoff, 
                TARGET.IdCompRound = source.id_comp_round,
                TARGET.idPayee = SOURCE.idPayee,
                TARGET.FirstName = SOURCE.EmployeeFirstName, 
                TARGET.LastName = SOURCE.EmployeeLastName, 
                TARGET.EmployeeStatusStartDate = SOURCE.EmployeeStatusStartDate, 
                TARGET.EmployeeStatusEndDate = SOURCE.EmployeeStatusEndDate, 
                TARGET.TerminationDate = SOURCE.DateTermination,
                TARGET.OrganisationalUnit = SOURCE.OrganizationName, 
                TARGET.CountryCompanyCode = SOURCE.CompanyCountryCode,
                TARGET.IdCompanyCode = SOURCE.Company_ID, 
                TARGET.EntryInCompany = SOURCE.HireDate, 
                TARGET.LineManager = SOURCE.LineManager1EmployeeCode, 
                TARGET.SecondLineManager = SOURCE.LineManager2EmployeeCode, 
                TARGET.EmployeeGroup = SOURCE.EmployeeGroupName, 
                TARGET.EmployeeSubGroup = SOURCE.EmployeeSubGroupName, 
                TARGET.CostCenter = SOURCE.CostCenterName, 
                TARGET.PayrollArea = SOURCE.PayrollAreaName,
				TARGET.PersonnelAreaCode = SOURCE.PersonnelAreaCode,
                TARGET.PersonnelAreaName = SOURCE.PersonnelAreaName,
				TARGET.PersonnelSubAreaCode = SOURCE.PersonnelSubAreaCode,
                TARGET.PersonnelSubAreaName = SOURCE.PersonnelSubAreaName,
                TARGET.PositionCode = SOURCE.PositionCode,
				TARGET.PositionName = SOURCE.PositionName,
                TARGET.IdPositionGrade = SOURCE.IdPositionGrade, 
                TARGET.PositionGradeCode = SOURCE.PositionGradeCode,
                TARGET.IdPersonalGrade = SOURCE.IdPersonalGrade,
                TARGET.PersonalGradeCode = SOURCE.PersonalGradeCode,
                TARGET.Age = SOURCE.Age, 
                TARGET.Gender = SOURCE.GenderName,
                TARGET.WorkPercentage = SOURCE.WorkPercentage, 
                TARGET.Currency = SOURCE.CurrencySalary, 
                TARGET.AnnualBaseSalary = SOURCE.AnnualSalary, 
                TARGET.MonthlyBaseSalary = SOURCE.MonthlySalary, 
                TARGET.PreviousIncreaseDate = SOURCE.PreviousIncreaseDate,
                TARGET.ScopeCode = SOURCE.scope_code,
                TARGET.IdStandardJob = SOURCE.IdStandardJob, 
                TARGET.StandardJobCode = SOURCE.StandardJobCode,
                TARGET.idPerformanceRating = SOURCE.idPerformanceRating, 
                TARGET.PerformanceCode = SOURCE.PerformanceCode, 
                TARGET.IdTalentRating = SOURCE.IdTalentRating, 
                TARGET.TalentCode = SOURCE.TalentCode,
                TARGET.CompanyCode = SOURCE.CompanyCode,
                TARGET.NbOfMonths = SOURCE.NbOfMonths,
                TARGET.FunctionCode = SOURCE.FunctionCode,
				TARGET.FunctionName = SOURCE.FunctionName

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (IdCompRound, IdScope, ScopeCode, IdMeritProcess, EligibilityCutoffDate, EmployeeDataCutoffDate, idPayee, EmployeeCode, 
                    FirstName, LastName, EmployeeStatusAtEligibilityCutoffDate, EmployeeStatusStartDate, EmployeeStatusEndDate, TerminationDate, 
                    OrganisationalUnit, CountryCompanyCode, IdCompanyCode, CompanyCode, EntryInCompany, LineManager, 
                    SecondLineManager, EmployeeGroup, EmployeeSubGroup, CostCenter, PayrollArea, NbOfMonths,
                    PersonnelAreaName, PersonnelAreaCode, PersonnelSubAreaName, PersonnelSubAreaCode, PositionName, PositionCode, 
					IdPositionGrade, PositionGradeCode, IdPersonalGrade, PersonalGradeCode, Age, 
                    Gender, WorkPercentage, Currency, AnnualBaseSalary, MonthlyBaseSalary, PreviousIncreaseDate,
					FunctionCode, FunctionName)
                VALUES (SOURCE.id_comp_round, SOURCE.id_scope, source.scope_code, SOURCE.idMeritProcess, SOURCE.eligibility_cutoff_date, SOURCE.employee_data_cutoff, SOURCE.idPayee, SOURCE.EmployeeCode,
                   SOURCE.EmployeeFirstName, SOURCE.EmployeeLastName, SOURCE.StatusEmployeeCode, SOURCE.EmployeeStatusStartDate, SOURCE.EmployeeStatusEndDate, SOURCE.DateTermination,
                   SOURCE.OrganizationName, SOURCE.CompanyCountryCode, SOURCE.Company_ID, SOURCE.CompanyCode, SOURCE.HireDate, SOURCE.LineManager1EmployeeCode, 
                   SOURCE.LineManager2EmployeeCode, SOURCE.EmployeeGroupName, SOURCE.EmployeeSubGroupName, SOURCE.CostCenterName, SOURCE.PayrollAreaName, SOURCE.NbOfMonths,
                   SOURCE.PersonnelAreaName, SOURCE.PersonnelAreaCode, SOURCE.PersonnelSubAreaName, SOURCE.PersonnelSubAreaCode, SOURCE.PositionName, SOURCE.PositionCode,
				   SOURCE.IdPositionGrade, SOURCE.PositionGradeCode, SOURCE.IdPersonalGrade, SOURCE.PersonalGradeCode, SOURCE.Age, 
                   SOURCE.GenderName, SOURCE.WorkPercentage, SOURCE.CurrencySalary, SOURCE.AnnualSalary, SOURCE.MonthlySalary, SOURCE.PreviousIncreaseDate,
				   SOURCE.FunctionCode, SOURCE.FunctionName)

            WHEN NOT MATCHED BY SOURCE THEN
                DELETE

            OUTPUT $action, COALESCE(deleted.EmployeeCode, inserted.EmployeeCode ) INTO @mergeAct ;

                /** Logs - merge **/
                SET @ins = COALESCE(( SELECT COUNT(ModifiedCode) AS rowsAffected FROM @mergeAct WHERE mrgAction = 'INSERT' GROUP BY mrgAction ), 0)
                SET @upd = COALESCE(( SELECT COUNT(ModifiedCode) AS rowsAffected FROM @mergeAct WHERE mrgAction = 'UPDATE' GROUP BY mrgAction ), 0)
                SET @del = COALESCE(( SELECT COUNT(ModifiedCode) AS rowsAffected FROM @mergeAct WHERE mrgAction = 'DELETE' GROUP BY mrgAction ), 0)            
                SET @EventText = concat('Employee data MERGE target: dbo.tb_mi_eligibility | INSERTED rows: ', @ins, ', UPDATED rows: ', @upd, ', DELETED rows: ', @del)
                EXEC [sp_audit_log] @Category, @Process, @SubProcess, @StartDate, @EventText, @AuditId, @UserId


        /** Status merge **/
        ;
        WITH eligib_source AS ( 

            /** Get the list of employees belonging to the scope defined in MI Process Definition which are valid at the eligibility cutoff date. **/
            SELECT miPd.idMeritProcess, emSi.EmployeeCode, emSi.StatusEmployeeCode, 
                   CASE WHEN emSi.StatusEmployeeCode = 0 THEN 1 ELSE NULL END AS HiddenFlag
            FROM dbo.tb_mi_process_definition miPd
            INNER JOIN dbo._tb_bqm_Link_Company_to_Scope lcts
                ON miPd.id_scope = lcts.Scope_ID
            INNER JOIN dbo._ep_dm_ref_company rfCo
                ON lcts.Company_ID = rfCo.Company_ID
            INNER JOIN dbo.tb_mi_employee_situation emSi
                ON rfCo.Company_Code = emSi.CompanyCode
            WHERE miPd.id_status <> 3                                                    -- Merit process status is different than ARCHIVE
                AND emSi.StatusEmployeeCode IS NOT NULL
                AND miPd.eligibility_cutoff_date BETWEEN emSi.StartDate AND emSi.EndDate
                AND ( emSi.DateTermination >= miPd.eligibility_cutoff_date OR emSi.DateTermination IS NULL )
                AND ( miPd.id_scope = @id_scope OR @id_scope = -1 )
                AND ( miPd.idMeritProcess = @idMeritProcess OR @idMeritProcess = -1 )
        ),

        eligib_target AS ( 
            SELECT elg.IdMeritProcess, elg.EmployeeCode, elg.EmployeeStatusAtEligibilityCutoffDate, elg.HiddenFlag, elg.HiddenFlagFrozen
            FROM dbo.tb_mi_eligibility elg
            INNER JOIN dbo.tb_mi_process_definition miPd
                ON elg.IdMeritProcess = miPd.IdMeritProcess
                AND ( miPd.idMeritProcess = @idMeritProcess OR @idMeritProcess = -1 )
                AND miPd.id_status <> 3                                                        -- Merit process status is different than ARCHIVE
        )

            MERGE eligib_target AS TARGET
            USING eligib_source AS SOURCE
                ON TARGET.idMeritProcess = SOURCE.idMeritProcess
                AND TARGET.EmployeeCode = SOURCE.EmployeeCode

            WHEN MATCHED 
			THEN UPDATE SET 
                TARGET.EmployeeStatusAtEligibilityCutoffDate = SOURCE.StatusEmployeeCode, 
                TARGET.HiddenFlag = CASE WHEN TARGET.HiddenFlagFrozen = 1 THEN TARGET.HiddenFlag ELSE SOURCE.HiddenFlag END

            OUTPUT $action, COALESCE(deleted.EmployeeCode, inserted.EmployeeCode ) INTO @mergeAct ;

                /** Logs - merge **/
                SET @ins = COALESCE(( SELECT COUNT(ModifiedCode) AS rowsAffected FROM @mergeAct WHERE mrgAction = 'INSERT' GROUP BY mrgAction ), 0)
                SET @upd = COALESCE(( SELECT COUNT(ModifiedCode) AS rowsAffected FROM @mergeAct WHERE mrgAction = 'UPDATE' GROUP BY mrgAction ), 0)
                SET @del = COALESCE(( SELECT COUNT(ModifiedCode) AS rowsAffected FROM @mergeAct WHERE mrgAction = 'DELETE' GROUP BY mrgAction ), 0)            
                SET @EventText = concat('Status MERGE target: dbo.tb_mi_eligibility | INSERTED rows: ', @ins, ', UPDATED rows: ', @upd, ', DELETED rows: ', @del)
                EXEC [sp_audit_log] @Category, @Process, @SubProcess, @StartDate, @EventText, @AuditId, @UserId
                DELETE FROM @mergeAct


        /**** Logs ****/
        EXEC [sp_audit_log]
            @Category = @Category,
            @Process  = @Process,
            @SubProcess = @SubProcess,
            @StartDate = @StartDate,
            @EventText = 'Procedure end',
            @AuditId = -1,
            @UserId = @UserId

        COMMIT TRANSACTION
    END TRY

    BEGIN CATCH

        DECLARE @xstate INT = XACT_STATE()

        IF @xstate != 0
            ROLLBACK TRANSACTION

        /**** Logs ****/
        DECLARE @ErrorFlag BIT = 1;
        DECLARE @ErrorText NVARCHAR(MAX) = ERROR_MESSAGE();
        DECLARE @ErrorLine INT = ERROR_LINE();

        EXEC [sp_audit_log]
            @Category = @Category,
            @Process  = @Process,
            @SubProcess = @SubProcess,
            @StartDate = @StartDate,
            @EventText = 'Error',
            @AuditId = -1,
            @UserId = @UserId,
            @ErrorFlag = @ErrorFlag,
            @ErrorText = @ErrorText,
            @ErrorLine = @ErrorLine    

        /*
        PRINT 'Transaction was rolled back!'
        PRINT ERROR_MESSAGE()
        PRINT concat( 'Line: ' , ERROR_LINE() )
        */

    END CATCH
END
GO


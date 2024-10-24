# pip install pyodbc
# pip install pandas openpyxl
# pip install sqlalchemy
# pip install xlwings
# pip install auto-py-to-exe

# Set ODBC Data Sources in the system
# 1. Search for app ODBC Data Sources
# 2. Change tab to System DNS
# 3. Add source



import sys
import tkinter
from tkinter import messagebox, ttk
import pyodbc 
from sqlalchemy import create_engine
import configparser
from pandas.core.frame import DataFrame
import pandas as pd 
import xlwings as xw
import os, shutil
from datetime import datetime



###   GLOBAL FUNCTIONS   ###

# Protect Excel sheets if they exist and aren't protected
def protectSheetIfExists(app, sheet):
    try: 
        if sheet == 'Master':
            app.sheets[sheet].api.Protect(Password='Password', AllowFormattingColumns=True, AllowFormattingRows=True, AllowFiltering=True)
        elif sheet == 'Faktury':
            app.sheets[sheet].api.Protect(Password='Password', AllowFiltering=True)
        else:
            app.sheets[sheet].api.Protect(Password='Password')
    except Exception as e:
        return False


# Unprotect Excel sheets if they exist and are protected
def unprotectSheetIfExists(app, sheet):
    try: 
        app.sheets[sheet].api.Unprotect(Password='Password')
    except Exception as e:
        return False



###   MAIN FUNCTION CODE   ###

def refreshMasters():

    returnMessage = ""
    notLockedFiles = []


    ###   RETRIVING DEFINITIONS FROM CONF FILES   ###

    # files.conf: get SQL environment info
    parser = configparser.RawConfigParser()
    fileDir = os.path.dirname(os.path.realpath(__file__))+'/files/'
    parser.read(fileDir+"files.conf", encoding="utf-8")
    section = "ENVIRONMENT"
    environment = parser.get(section, "environment")

    # files.conf: get paths of superMasterExcel file and local temporary directory masterTempDir
    section = "OPERAC_"+environment
    superMasterExcel = parser.get(section, "superMaster")
    masterTempDir = parser.get(section, "masterTempDir")
    masterBackupDir = parser.get(section, "masterBackupDir")
    budowyZestawienieExcel = parser.get(section, "budowyZestawienie")

    # files.conf: get paths of all Master files to be refreshed
    section = "MASTER_"+environment
    masterFiles = parser.items(section)
    
    # files.conf: get names of all construction sites defined in Optima to masterOptimaName array
    section = "OPTIMA_NAME"
    optimaName = parser.items(section)
    masterOptimaName = []
    for name in optimaName:
        masterOptimaName.append(name[1])

    # connection.conf: get SQL connection data
    parser.read(fileDir+"connection.conf")
    connServ = parser.get(environment, "connServ")
    connUser = parser.get(environment, "connUser")
    connPass = parser.get(environment, "connPass")
    connDbas = parser.get(environment, "connDbas")



    ###   SQL CONNECTIONS   ###

    # SQLAlchemy engine connection for bulk insert
    conn_str = f'mssql+pyodbc://{connUser}:{connPass}@{connServ}/{connDbas}?driver=ODBC+Driver+17+for+SQL+Server'
    alchEng = create_engine(conn_str)

    # ODBC connection
    connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={connServ};DATABASE={connDbas};UID={connUser};PWD={connPass}'
    try:
        sqlServer = pyodbc.connect(connectionString) 
        cursor = sqlServer.cursor()
        sqlServer.autocommit = True
    except: 
        return "ERROR! \nPróba połączenia z serwerem SQL zakończona niepowodzeniem. \nUpewnij się, że urządzenie działa i znajduje się w tej samej sieci."



    ###   LOAD SUPER_MASTER.XLS CONTENT TO SLQ TABLES   ###

    # Exec SP truncating SQL tables corresponding to Excel sheets in the file
    executesp = """EXEC DB_Constructo.Master_Src.SP_Truncate_Manualne_Faktury"""
    cursor.execute(executesp)
    sqlServer.commit()
    
    # Get content of Super_Master.xls file sheets
    fPrzeniesione = pd.read_excel(superMasterExcel, sheet_name = 'Faktury_Przeniesione', engine = 'openpyxl')
    fManualne = pd.read_excel(superMasterExcel, sheet_name = 'Faktury_Manualne', engine = 'openpyxl')
    fWynagrodzenie = pd.read_excel(superMasterExcel, sheet_name = 'Faktury_Wynagrodzenie', engine = 'openpyxl')
    fSprzedaz = pd.read_excel(superMasterExcel, sheet_name = 'Faktury_Sprzedaz', engine = 'openpyxl')
    wKontrakt = pd.read_excel(superMasterExcel, sheet_name = 'Wartosci_Kontraktu', engine = 'openpyxl')

    # Fill proper SQL tables with data from Super_Master.xls sheets
    try:
        fPrzeniesione.to_sql('T_Manualne_Faktury_Przeniesione', schema='Master_Src', con=alchEng, if_exists='append', index=False)
        fManualne.to_sql('T_Manualne_Faktury_Manualne', schema='Master_Src', con=alchEng, if_exists='append', index=False)
        fWynagrodzenie.to_sql('T_Manualne_Faktury_Wynagrodzenie', schema='Master_Src', con=alchEng, if_exists='append', index=False)
        fSprzedaz.to_sql('T_Manualne_Faktury_Sprzedaz', schema='Master_Src', con=alchEng, if_exists='append', index=False)
        wKontrakt.to_sql('T_Manualne_Wartosci_Kontraktu', schema='Master_Src', con=alchEng, if_exists='append', index=False)

    except Exception as e:
        message = str(e)
        if message.find("Cannot insert the value NULL into column") > 0:
            return "ERROR! \nUpewnij się, że wszystkie wpisy w pliku Super_Master.xls mają zdefiniowane wartości w kolumnach: \nDokument, Nip, Rejestr_Master, Razem_Netto!"
        else:
            return "ERROR! \nPodczas kopiowania danych z Super_Master.xls do tabel SQL wystąpił nieznany błąd! \nSkontaktuj się z administratorem."
    else:
        print("Super_Master file content uploaded to SQL DB.")



    ###   VALIDATIONS OF THE DATA PROVIDED BY THE USER IN SUPER_MASTER EXCEL FILE   ###

    # Data validation function. Checking uniqueness of the records in Super_Master excel file.
    def sqlValidationSuperMasterUnique(sql, msg):
        cursor.execute(executesql)
        result = cursor.fetchall()

        if len(result) > 0:
            rowsContent = ''
            for row in result:
                print(row)
                rowsContent += '\nVaV_Dokument: ' + row[0] + ',     VaV_KntNipE: ' + row[1]
            return msg + rowsContent
        else:
            return False
			
	# Data uniqueness validation for Super_Master > Faktury_Przeniesione	
    executesql = """SELECT DISTINCT VaV_Dokument, VaV_KntNipE FROM (
	                    SELECT VaV_Dokument, VaV_Wariant, VaV_KntNipE, COUNT(*) AS cnt
	                    FROM DB_Constructo.Master_Src.T_Manualne_Faktury_Przeniesione
	                    GROUP BY VaV_Dokument, VaV_Wariant, VaV_KntNipE
	                    HAVING COUNT(*) > 1
                   ) cnt"""
    msg = "ERROR!!! \nPopraw: Super_Master > Faktury_Przeniesione \n\nKombinacja kolumn VaV_Dokument, VaV_KntNipE, VaV_Wariant musi być unikalna dla każdego wiersza. \nUsuń nadmiarowe rekordy lub dodaj unikalny numer porządkowy w kolumnie VaV_Wariant. \n"
    if (sqlValidationSuperMasterUnique(executesql, msg) != False):
        return sqlValidationSuperMasterUnique(executesql, msg)

    # Data uniqueness validation for Super_Master > Faktury_Manualne	
    executesql = """ SELECT DISTINCT VaV_Dokument, VaV_KntNipE FROM (
	                    SELECT VaV_Dokument, VaV_KntNipE, COUNT(*) AS cnt
	                    FROM DB_Constructo.Master_Src.T_Manualne_Faktury_Manualne
	                    GROUP BY VaV_Dokument, VaV_KntNipE
	                    HAVING COUNT(*) > 1
                   ) cnt"""
    msg = "ERROR!!! \nPopraw: Super_Master > Faktury_Manualne \n\nKombinacja kolumn VaV_Dokument, VaV_KntNipE musi być unikalna dla każdego wiersza. \nUsuń nadmiarowe rekordy lub zmień wartości w kolumnach VaV_Dokument, VaV_KntNipE. \n"
    if (sqlValidationSuperMasterUnique(executesql, msg) != False):
        return sqlValidationSuperMasterUnique(executesql, msg)
    
    # Data uniqueness validation for Super_Master > Faktury_Wynagrodzenie
    executesql = """ SELECT DISTINCT VaV_Dokument, VaV_KntNipE FROM (
	                    SELECT VaV_Dokument, VaV_KntNipE, COUNT(*) AS cnt
	                    FROM DB_Constructo.Master_Src.T_Manualne_Faktury_Wynagrodzenie
	                    GROUP BY VaV_Dokument, VaV_KntNipE
	                    HAVING COUNT(*) > 1
                   ) cnt"""
    msg = msg.replace('Faktury_Manualne','Faktury_Wynagrodzenie')
    if (sqlValidationSuperMasterUnique(executesql, msg) != False):
        return sqlValidationSuperMasterUnique(executesql, msg)

    # Data uniqueness validation for Super_Master > Faktury_Sprzedaz
    executesql = """ SELECT DISTINCT VaV_Dokument, VaV_KntNipE FROM (
	                    SELECT VaV_Dokument, VaV_KntNipE, COUNT(*) AS cnt
	                    FROM DB_Constructo.Master_Src.T_Manualne_Faktury_Sprzedaz
	                    GROUP BY VaV_Dokument, VaV_KntNipE
	                    HAVING COUNT(*) > 1
                   ) cnt"""
    msg = msg.replace('Faktury_Wynagrodzenie','Faktury_Sprzedaz')
    if (sqlValidationSuperMasterUnique(executesql, msg) != False):
        return sqlValidationSuperMasterUnique(executesql, msg)
    
    
    # Data validation function. Checking if the names entered by user in Rejestr (Budowa) column of Super_Master file are compliant with Optima system Rejestr names
    def sqlValidationSuperMasterRejestr(sqlColumn, sqlTable, sheet):
        masterSqlName = []
        executesql = """SELECT DISTINCT """ + sqlColumn + """ FROM DB_Constructo.Master_Src.""" + sqlTable
        cursor.execute(executesql)
        result = cursor.fetchall()

        for row in result:
            masterSqlName.append(row[0])

        masterOptimaNamePlus = masterOptimaName
        masterOptimaNamePlus.append('KOSZTY')
        masterOptimaNamePlus.append('INNE')
        masterOptimaNamePlus.append('SPRZEDAŻ')
        masterOptimaNamePlus.append('LUBOŃ')
        masterOptimaNamePlus.append('NEKLA')
        masterOptimaNamePlus.append('NOWY TOMYŚL')
        masterOptimaNamePlus.append('ZKZL')

        for i in range(len(masterSqlName)):
            try:
                masterOptimaNamePlus.index(masterSqlName[i])
            except:
                return "ERROR!!! \nPopraw: Super_Master > " + sheet + " \n\n" + sqlColumn + " '" + masterSqlName[i] + "' nie istnieje w bazie danych Optima. Upewnij się, że nazwa została poprawnie wprowadzona."
        
        return False

    # Checking if column VaV_Rejestr_Optima in the table T_Manualne_Faktury_Przeniesione contains valid names
    if (sqlValidationSuperMasterRejestr('VaV_Rejestr_Optima', 'T_Manualne_Faktury_Przeniesione', 'Faktury_Przeniesione') != False):
        return sqlValidationSuperMasterRejestr('VaV_Rejestr_Optima', 'T_Manualne_Faktury_Przeniesione', 'Faktury_Przeniesione')

    # Checking if column VaV_Rejestr_Master in the table T_Manualne_Faktury_Przeniesione contains valid names
    if (sqlValidationSuperMasterRejestr('VaV_Rejestr_Master', 'T_Manualne_Faktury_Przeniesione', 'Faktury_Przeniesione') != False):
        return sqlValidationSuperMasterRejestr('VaV_Rejestr_Master', 'T_Manualne_Faktury_Przeniesione', 'Faktury_Przeniesione')
    
    # Checking if column VaV_Rejestr_Master in the table T_Manualne_Faktury_Manualne contains valid names
    if (sqlValidationSuperMasterRejestr('VaV_Rejestr_Master', 'T_Manualne_Faktury_Manualne', 'Faktury_Manualne') != False):
        return sqlValidationSuperMasterRejestr('VaV_Rejestr_Master', 'T_Manualne_Faktury_Manualne', 'Faktury_Manualne')
    
    # Checking if column VaV_Rejestr_Master in the table T_Manualne_Faktury_Wynagrodzenie contains valid names
    if (sqlValidationSuperMasterRejestr('VaV_Rejestr_Master', 'T_Manualne_Faktury_Wynagrodzenie', 'Faktury_Wynagrodzenie') != False):
        return sqlValidationSuperMasterRejestr('VaV_Rejestr_Master', 'T_Manualne_Faktury_Wynagrodzenie', 'Faktury_Wynagrodzenie')
    
    # Checking if column VaV_Rejestr_Optima in the table T_Manualne_Faktury_Sprzedaz contains valid names
    if (sqlValidationSuperMasterRejestr('VaV_Rejestr_Optima', 'T_Manualne_Faktury_Sprzedaz', 'Faktury_Sprzedaz') != False):
        return sqlValidationSuperMasterRejestr('VaV_Rejestr_Optima', 'T_Manualne_Faktury_Sprzedaz', 'Faktury_Sprzedaz')
    
    # Checking if column VaV_Rejestr_Master in the table T_Manualne_Faktury_Sprzedaz contains valid names
    if (sqlValidationSuperMasterRejestr('VaV_Rejestr_Master', 'T_Manualne_Faktury_Sprzedaz', 'Faktury_Sprzedaz') != False):
        return sqlValidationSuperMasterRejestr('VaV_Rejestr_Master', 'T_Manualne_Faktury_Sprzedaz', 'Faktury_Sprzedaz')
    
    # Checking if column Budowa in the table T_Manualne_Wartosci_Kontraktu contains valid names
    if (sqlValidationSuperMasterRejestr('Budowa', 'T_Manualne_Wartosci_Kontraktu', 'Wartosci_Kontraktu') != False):
        return sqlValidationSuperMasterRejestr('Budowa', 'T_Manualne_Wartosci_Kontraktu', 'Wartosci_Kontraktu')
    


    ###   MERGE SYSTEM DATA AND SUPER_MASTER   ###

    # Run SP which updates the SQL source table for all the Masters Master_Src.T_Master_Faktury    
    executesp = """EXEC DB_Constructo.Master_Src.SP_Merge_Master_Faktury"""
    cursor.execute(executesp)
    sqlServer.commit()



    ###   MASTER EXCELS - UPLOAD FAKTURY SHEET TO SQL   ###
    
    i = 0

    for file in masterFiles:

        # Define SQL table name corresponding to every refreshed Master file basing on masterOptimaName array
        tableName = f'T_Master_Excel_Przypisanie_{masterOptimaName[i]}'
        tableName = tableName.replace('-','').replace('.','_').replace(' ','_')
        tableName = tableName.replace('__','_')
        tableName = tableName.replace('Ą','A').replace('Ć','C').replace('Ę','E').replace('Ł','L').replace('Ń','N').replace('Ó','O').replace('Ś','S').replace('Ź','Z').replace('Ż','Z')

        # Try to read the content of Masters' Faktury sheet. If the file is busy skip the processing 
        try:  
            fPrzeniesione = pd.read_excel(file[1], sheet_name = 'Faktury', engine = 'openpyxl')

        except Exception as e:
            message = str(e)
            if message.find("Permission denied") > 0:
                returnMessage += "\nWARNING! \nPlik o podanej ścieżce NIE został odświeżony ponieważ jest obecnie edytowany przez inną osobę: \n"+file[1]
            elif message.find("No such file") > 0:
                return "ERROR! \nPlik o podanej ścieżce nie istnieje: \n"+file[1]+" \nProces nie będzie kontynuowany! \nUpewnij się, że ścieżka zdefiniowana w files.conf jest poprawna i uruchom proces ponownie."
            else:
                return "ERROR! \n"+str(e)
        
        else:
            # Array notLockedFiles contains only the files which are not locked by another user
            notLockedFiles.append(file[1])
            # Load Faktury sheet to SQL table
            fPrzeniesione.to_sql(tableName, schema='Master_Src', con=alchEng, if_exists='replace', index=False)
            # Column [Unnamed: 8] populated from the Excel file contains the Comment. It gets replaced with masterOptimaName to be able to identify the source Excel file in SQL DB
            executeSql = f"UPDATE DB_Constructo.Master_Src.[" + tableName + "] SET [Unnamed: 8] = '" + masterOptimaName[i] + "'"
            cursor.execute(executeSql)
            sqlServer.commit()

            i = i+1
            print("Table updated: " + tableName)

    # Procedure that combines all SQL tables created by above code into one table T_Master_Excel_Przypisanie
    executesp = """EXEC DB_Constructo.Master_Src.SP_Refresh_Master_Excel_Przypisanie"""
    cursor.execute(executesp)
    sqlServer.commit()



    ###   MASTER EXCELS - UPLOAD FAKTURY SHEET TO SQL   ###

    datePrefix = datetime.now().strftime('%y%m%d_%H%M_')

    for file in masterFiles:

        try:
            # If the file is locked by other user, below statement will cause an error
            notLockedFiles.index(file[1])
        except: 
            print("File omitted: "+file[1])
        else:
            # Save the backup copy of refreshed file in masterTempDir and then move it replacing the initial file
            fileName = os.path.basename(file[1])
            wbk = xw.Book(file[1], update_links=False)
            backupFile = masterBackupDir+datePrefix+fileName
            wbk.save(backupFile)
            
            # Unprotect the file, refresh all connections and protect the file
            print('Starting update of: ' + file[1])
            wbk.api.Unprotect(Password='Password')
            unprotectSheetIfExists(wbk, 'Master') 
            unprotectSheetIfExists(wbk, 'Faktury') 
            unprotectSheetIfExists(wbk, 'Podsumowanie')
            wbk.api.RefreshAll()
            wbk.api.Protect(Password='Password')
            protectSheetIfExists(wbk, 'Master') 
            protectSheetIfExists(wbk, 'Faktury') 
            protectSheetIfExists(wbk, 'Podsumowanie') 

            # Save the temporary copy of refreshed file in masterTempDir and then move it replacing the initial file
            tempFile = masterTempDir+fileName
            wbk.save(tempFile)
            app = xw.apps.active
            app.quit()
            shutil.move(tempFile, file[1])
            print(fileName+' updated')



    ###   REFRESH BUDOWY ZESTAWIENIE   ###

    # Try to read excel file budowyZestawienieExcel. Skip file refresh if it's locked, proceed with the refresh if it's free.
    try:
        wbk = xw.Book(budowyZestawienieExcel, update_links=False)
        #wbkRead = pd.read_excel(wbk, sheet_name = 'ZESTAWIENIE 2024', engine = 'openpyxl')
    except Exception as e:
        message = str(e)
        if message.find("Permission denied") > 0:
            returnMessage += "\nWARNING! \nPlik o podanej ścieżce NIE został odświeżony ponieważ jest obecnie edytowany przez inną osobę: \n"+file[1]
        elif message.find("No such file") > 0:
            return "ERROR! \nPlik o podanej ścieżce nie istnieje: \n"+file[1]+" \nProces nie będzie kontynuowany! \nUpewnij się, że ścieżka zdefiniowana w files.conf jest poprawna i uruchom proces ponownie."
        else:
            return "ERROR! \n"+str(e)
    else:
        wbk.api.RefreshAll()

        # Save the temporary copy of refreshed file in masterTempDir and then move it replacing the initial file
        fileName = os.path.basename(budowyZestawienieExcel)
        tempFile = masterTempDir+fileName
        wbk.save(tempFile)
        app = xw.apps.active
        app.quit()
        shutil.move(tempFile, budowyZestawienieExcel)
        print('Budowy Zestawienie updated')

    # Final message if entire script whent through without errors
    return "Odświeżanie Masterów zakończone powodzeniem. \n" + returnMessage



###   GUI DEFINITION AND INVOKE   ###

class Application(tkinter.Frame):
    def __init__(self, master=None):
        tkinter.Frame.__init__(self, master)
        self.button = tkinter.Button(master, text="Start", command=self.getvalue)
        master.geometry("800x300")
        master.title("Odświeżanie Masterów")
        self.button.place(x=350, y=250, width = 100, height = 30)
        self.label1 = tkinter.Label(master, text="Pamiętaj, aby przed rozpoczęciem odświeżania zamknąć wszystkie uruchomione pliki Excel! \n\nNaciśnij przycisk poniżej aby uruchomić proces", justify="left", wraplength=740)
        self.label1.place(x=30, y=20, width = 740, height = 200)

    def getvalue(self):
        self.button.destroy()
        returnMessage = refreshMasters()
        self.label1.configure(text=returnMessage)
        
app = Application(tkinter.Tk())
app.mainloop()


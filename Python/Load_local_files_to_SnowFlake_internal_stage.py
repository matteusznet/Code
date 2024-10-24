
import snowflake.connector;
import shutil
import os
import tkinter
from tkinter import messagebox, ttk


# UI window and its elements

window = tkinter.Tk()
window.config(width=350, height=150)
window.title("Refresh SnowFlake masterdata")

label1 = tkinter.Label(window, text="Select or type your SnowFlake user name:")
label1.place(x=55, y=20)

combo = ttk.Combobox(
    values=["mail_1@example.com", "mail_2@example.com"]
)
combo.config(width=30, height=50)
combo.place(x=70, y=50)
combo.current(0)


# SnowFlake load functions

def loadFilesToSnowFlake(userMail):

    # SnowFlake connection
    conn = snowflake.connector.connect(
        user=f'{userMail}',
        authenticator='externalbrowser',
        account='ACC_NAME',
        warehouse='WH_NAME',
        database='DB_NAME',
        schema='SCH_NAME'
        )

    cur = conn.cursor()

    # Source files directory and SF stage
    stgName = "@INT_STG_NAME"
    fileDir = "//lux.intra.lighting.com/PL-PIL001/FPA-RA3/01 Backend/06_MPR Rebuild/SnowFlake/Masterdata files/"

    # Creating local directory without spaces in the path - SF put cannot handle spaces 
    userName = os.getlogin()
    copyFileParentDir = "C:/Users/" + userName + "/Documents/"
    copyFileDir = os.path.join(copyFileParentDir, "SF_Manuals/")

    if not os.path.exists(copyFileDir): 
        os.mkdir(copyFileDir)

    shutil.copytree(fileDir, copyFileDir, dirs_exist_ok=True)

    # Uploading files to SnowFlake
    allFiles = copyFileDir + "*"
    uploadStmt = f'PUT file://{allFiles} @INT_STG_NAME OVERWRITE = TRUE;'
    cur.execute(uploadStmt);

    # Exec SF stored procedure
    runSp = 'CALL DB_NAME.SCH_NAME.SP_TEST()'
    cur.execute(runSp);

def buttonAction():
    selectedCombo = combo.get()
    loadFilesToSnowFlake(selectedCombo)
    window.quit();

button = ttk.Button(text="Load Files", command=buttonAction)
button.place(x=130, y=95)



# Start the event loop.
window.mainloop()


#auto-py-to-exe

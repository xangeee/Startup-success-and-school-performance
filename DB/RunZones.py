import os
import sys
import duckdb
import time
import warnings
warnings.filterwarnings("ignore")
import urllib.request

from tkinter.tix import *


from tkinter import *
from tkinter import ttk

main_dir = os.getcwd() #DB
master = Tk()

container = ttk.Frame(master)
canvas = Canvas(container)


scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
scrollable_frame = ttk.Frame(canvas)

scrollable_frame.bind(
    "<Configure>",
    lambda e: canvas.configure(
        scrollregion=canvas.bbox("all")
    )
)

canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")

canvas.configure(yscrollcommand=scrollbar.set)
container.pack()
canvas.pack(side="left", fill="both", expand=True)
scrollbar.pack(side="right", fill="y")



def my_mainloop():
        
        try:
            #Formatted zone
            print("\n")
            print("\n")
            print("################################ Starting process ################################# \n")
            ttk.Label(scrollable_frame, text="################################ Starting process ################################# \n").pack()
            os.system("python ../FORMATTED_ZONE/LoadingData_FormattedZone.py")
            print("**** All the data has been moved to FORMATTED ZONE ****\n")
            ttk.Label(scrollable_frame, text="**** All the data has been moved to FORMATTED ZONE ****\n").pack()
            datasets_fz = []
            conn = duckdb.connect("../DB/DB_FormattedZone", read_only=False)
            tables_FZ=conn.execute("SHOW TABLES").fetchall()
            conn.close()
            datasets_tz = []
            conn = duckdb.connect("../DB/DB_TrustedZone", read_only=False)
            tables_TZ=conn.execute("SHOW TABLES").fetchall()
            conn.close()
            
            for d1, d2 in zip(tables_FZ, tables_TZ):
                datasets_fz.append(d1[0])
                datasets_fz.append(d2[0])
                
            if set(datasets_fz) != set(datasets_tz):
                os.system("python ../TRUSTED_ZONE/LoadingData_TrustedZone.py")
                
                print("**** All the data have been moved to TRUSTED ZONE ****\n")
                ttk.Label(scrollable_frame, text="**** All the data have been moved to TRUSTED ZONE ****\n").pack()
                os.system("python ../TRUSTED_ZONE/Outliers.py")
                os.system("python ../TRUSTED_ZONE/schools2018_QualityTasks.py")
                os.system("python ../TRUSTED_ZONE/schoolsUbication_QualityTasks.py")
                os.system("python ../TRUSTED_ZONE/startups_QualityTasks.py")
                print("**** Data quality tasks process finished ****\n")
                ttk.Label(scrollable_frame, text="**** Data quality tasks process finished ****\n").pack()
                #explotation zone
                os.system("python ../EXPLOTATION_ZONE/LoadingData_ExplotationZone.py")
                os.system("python ../EXPLOTATION_ZONE/Pre_data_integration.py")
                os.system("python ../EXPLOTATION_ZONE/DataIntegration.py")
                print("**** All the data is integrated in EXPLOTATION ZONE ****\n")
                ttk.Label(scrollable_frame, text="**** All the data is integrated in EXPLOTATION ZONE ****\n").pack()
            else:
                print("There is no new datasource. All data has been treated! \n")
                ttk.Label(scrollable_frame, text="There is no new datasource. All data has been treated! \n").pack()
           
            
            print("################################################################################### \n")
            ttk.Label(scrollable_frame, text="################################################################################### \n").pack()
           
        except OSError as err:
            print("OS error:", err)
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            raise
        master.after(1000, my_mainloop)  # run again after 1000ms (1s)

master.after(1000, my_mainloop) # run first time after 1000ms (1s)
master.mainloop()

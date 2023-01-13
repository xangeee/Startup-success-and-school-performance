import os
import sys
import duckdb
import time
import warnings
warnings.filterwarnings("ignore")
from tkinter.tix import *


from tkinter import *
from tkinter import ttk

main_dir = os.getcwd() #DB

    
try:
    #Formatted zone
    print("\n")
    print("\n")
    print("################################ Starting process ################################# \n")
    print("**** EXECUTING DATA MANAGEMENT BACKBONE SCRIPTS****\n")
    os.system("python ../DATA_MANAGEMENT_BACKBONE/FORMATTED_ZONE/LoadingData_FormattedZone.py")
    print("**** All the data has been moved to FORMATTED ZONE ****\n")
    print("**** All the data has been moved to FORMATTED ZONE ****\n")
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
        os.system("python ../DATA_MANAGEMENT_BACKBONE/TRUSTED_ZONE/LoadingData_TrustedZone.py")
        
        print("**** All the data have been moved to TRUSTED ZONE ****\n")
        os.system("python ../DATA MANAGEMENT BACKBONE/TRUSTED_ZONE/Outliers.py")
        os.system("python ../DATA MANAGEMENT BACKBONE/TRUSTED_ZONE/schools2018_QualityTasks.py")
        os.system("python ../DATA MANAGEMENT BACKBONE/TRUSTED_ZONE/schoolsUbication_QualityTasks.py")
        os.system("python ../DATA MANAGEMENT BACKBONE/TRUSTED_ZONE/startups_QualityTasks.py")
        print("**** Data quality tasks process finished ****\n")
        #explotation zone
        os.system("python ../DATA MANAGEMENT BACKBONE/EXPLOTATION_ZONE/LoadingData_ExplotationZone.py")
        os.system("python ../DATA MANAGEMENT BACKBONE/EXPLOTATION_ZONE/Pre_data_integration.py")
        os.system("python ../DATA MANAGEMENT BACKBONE/EXPLOTATION_ZONE/DataIntegration.py")
        print("**** All the data is integrated in EXPLOTATION ZONE ****\n")
    else:
        print("There is no new datasource. All data has been treated! \n")
    
    print("**** EXECUTING DATA MANAGEMENT BACKBONE SCRIPTS****\n")
    os.system("python ../DATA_ANALYSIS_BACKBONE/Feature_Generation_Zone/0_LoadingData_Clean.py")
    os.system("python ../DATA_ANALYSIS_BACKBONE/Feature_Generation_Zone/1_DataPreparation.py")
    os.system("python ../DATA_ANALYSIS_BACKBONE/Feature_Generation_Zone/2_FeatureTransformation.py")
    os.system("python ../DATA_ANALYSIS_BACKBONE/Feature_Generation_Zone/3_FeatureSelection.py")

    os.system("python ../DATA_ANALYSIS_BACKBONE/Model_Training/0_LoadingData_Train_Test.py")
    os.system("python ../DATA_ANALYSIS_BACKBONE/Model_Training/TrainingModel.py")
    os.system("python ../DATA_ANALYSIS_BACKBONE/Model_Validating/ModelValidation.py")

    os.system("python ../DEPLOYMENT/modelIterface.py")
    os.system("python ../DEPLOYMENT/tests.py")
    
    print("################################################################################### \n")
    
    
except OSError as err:
    print("OS error:", err)
except Exception as err:
    print(f"Unexpected {err=}, {type(err)=}")
    raise
        



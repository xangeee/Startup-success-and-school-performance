import os
import sys
import duckdb
import time
import warnings
warnings.filterwarnings("ignore")

main_dir = os.getcwd() #DB

while True:
        try:
            #Formatted zone
            print("Starting process...")
            
            os.system("python ../FORMATTED_ZONE/LoadingData_FormattedZone.py")
            print("########  All data have been moved to FORMATTED ZONE  ########")
            
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
                
                print("########  All data have been moved to TRUSTED ZONE  ########")
                os.system("python ../TRUSTED_ZONE/Outliers.py")
                os.system("python ../TRUSTED_ZONE/schools2018_QualityTasks.py")
                os.system("python ../TRUSTED_ZONE/schoolsUbication_QualityTasks.py")
                os.system("python ../TRUSTED_ZONE/startups_QualityTasks.py")
                #explotation zone
                os.system("python ../EXPLOTATION_ZONE/LoadingData_ExplotationZone.py")
                os.system("python ../EXPLOTATION_ZONE/Pre_data_integration.py")
                os.system("python ../EXPLOTATION_ZONE/DataIntegration.py")
                print("########  The data is integrated in EXPLOTATION ZONE  ########")
            else:
                print("All data have been treated!")
        except OSError as err:
            print("OS error:", err)
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
            raise

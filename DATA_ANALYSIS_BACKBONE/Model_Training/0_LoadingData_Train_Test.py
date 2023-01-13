import duckdb
import pandas as pd
import os
import datetime

conn_fz = duckdb.connect("../DB/DB_FeatureGeneration",read_only=False)
conn_tz = duckdb.connect("../DB/DB_Train&Test",read_only=False)

def drop_table(conn, name):
        existingTables=conn.execute("SHOW TABLES").fetchall()
        if(len(existingTables)>0):
            for table in existingTables:
                if(table[0] == name):
                    conn.execute("DROP TABLE " + name)
def generate_table(conn, df, name):
        drop_table(conn, name)
        existingTables=conn.execute("SHOW TABLES").fetchall()
        if(len(existingTables)>0):
            for table in existingTables:
                if(table[0] == name):
                    conn.execute("DROP TABLE " + name)
        conn.execute("CREATE TABLE " + name + " AS SELECT * FROM df")

existingTables=conn_fz.execute("SHOW TABLES").fetchall()
for t in existingTables:
    query = "SELECT * from " + t[0]
    df = conn_fz.execute(query).fetchdf()
    #saving the data
    name = t[0].split("$")[0]
    if name == "train_data" or name == "test_data" :
      print(name)
      generate_table(conn_tz, df, name)


conn_tz.close()
conn_fz.close()


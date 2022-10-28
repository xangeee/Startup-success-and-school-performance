#!/usr/bin/env python
# coding: utf-8

# In[1]:


import duckdb
import pandas as pd
import numpy as np
import seaborn as sns
from sklearn.neighbors import LocalOutlierFactor
from sklearn.impute import SimpleImputer


# # Outliers detection

# In[2]:


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
        
def MultiOutliers(df):
    
    df_numeric = df.select_dtypes([np.number])
    imputer = SimpleImputer(missing_values=np.nan, strategy='mean')
    imputer = imputer.fit(df_numeric)

    df_numeric = imputer.transform(df_numeric)
    clf = LocalOutlierFactor(n_neighbors=20, contamination='auto')
    clf.fit_predict(df_numeric)
   
    y_pred = clf.negative_outlier_factor_

    # #outliers
    mask = y_pred ==-1

    df[mask].shape
    df["is_outlier"] = "No"
    df[mask] = "Yes"

    return df


# # Conexion to database

# In[3]:


conn_tz = duckdb.connect("../DB/DB_TrustedZone",read_only=False)


# In[4]:


existingTables=conn_tz.execute("SHOW TABLES").fetchall()
for t in existingTables:
    query = "SELECT * from " + t[0]
    table = t[0].split("$")[0]
    df = conn_tz.execute(query).fetchdf()
    if not ("schools2018" in t[0]):
        df = MultiOutliers(df)
    name = table + "$v2"
    generate_table(conn_tz, df, name)
    drop_table(conn_tz, t[0])


# In[5]:


conn_tz.close()


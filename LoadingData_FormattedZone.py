#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import duckdb
import os
from datetime import datetime


# In[2]:


conn = duckdb.connect("../DB/DB_FormattedZone", read_only=False)


# In[3]:


def dataset_union(df1_, df2_):
        df1 = pd.read_csv(df1_, encoding = "ISO-8859-1")
        df2 = pd.read_csv(df2_, encoding = "ISO-8859-1")
        cols = list(set(df1.columns) - set(df2.columns))
        df = df2
        df[cols] = df1[cols]
        return df 

def generate_table(conn, df, name):
        drop_table(conn, name)
        existingTables=conn.execute("SHOW TABLES").fetchall()
        if(len(existingTables)>0):
            for table in existingTables:
                if(table[0] == name):
                    conn.execute("DROP TABLE " + name)
        conn.execute("CREATE TABLE " + name + " AS SELECT * FROM df")

def drop_table(conn, name):
        existingTables=conn.execute("SHOW TABLES").fetchall()
        if(len(existingTables)>0):
            for table in existingTables:
                if(table[0] == name):
                    conn.execute("DROP TABLE " + name)


# In[4]:


dic = {}
dirc = "../LANDING/PERSISTENT/"

existingTables=conn.execute("SHOW TABLES").fetchall()

for f in os.listdir(dirc):
    if "csv" in f:
        name = f[:-4].replace("-", "_")
        if name not in existingTables:
            aux = f.split("$")[0]
            if f.split("$")[0] in dic.keys():
                dic[aux] = dic[aux] + [f]
            else:
                dic[aux] = [f]
            df = pd.read_csv(dirc + f, encoding = "ISO-8859-1")
            generate_table(conn, df, name)


# In[5]:


for x in dic:
    if len(dic[x]) > 1:
        df = dataset_union(dirc + dic[x][0], dirc + dic[x][1])
        name = x + "$" + datetime.now().strftime("%d/%m/%Y").replace("/", "_")
        generate_table(conn, df, name)
        for ff in dic[x]:
            ff = ff[:-4].replace("-", "_")
            drop_table(conn, ff)


# In[6]:


existingTables=conn.execute("SHOW TABLES").fetchall()
# print(existingTables)


# In[7]:


conn.close()


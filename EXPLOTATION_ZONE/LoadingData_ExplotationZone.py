#!/usr/bin/env python
# coding: utf-8

# In[1]:


import duckdb
import pandas as pd
import os
from IPython.display import display
import datetime


# In[2]:


conn_tz = duckdb.connect("../DB/DB_TrustedZone",read_only=False)
conn_ez = duckdb.connect("../DB/DB_ExplotationZone",read_only=False)


# In[3]:


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


# In[4]:


existingTables=conn_tz.execute("SHOW TABLES").fetchall()
for t in existingTables:
    query = "SELECT * from " + t[0]
    df = conn_tz.execute(query).fetchdf()
    #saving the data
    name = t[0].split("$")[0]
    generate_table(conn_ez, df, name)


# In[5]:


conn_tz.close()
conn_ez.close()


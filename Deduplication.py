#!/usr/bin/env python
# coding: utf-8

# In[1]:


import duckdb
import pandas as pd
import os
from IPython.display import display
import numpy as np
import duckdb
import datetime


# In[2]:


get_ipython().run_cell_magic('javascript', '', 'IPython.OutputArea.auto_scroll_threshold = 999999999;')


# In[3]:


from IPython.display import Markdown, display
def printmd(string):
    display(Markdown(string))


# ### Reading tables from database

# In[4]:


conn = duckdb.connect("../DB/DB_TrustedZone",read_only=False)


# In[5]:


dataframes={}
existingTables=conn.execute("SHOW TABLES").fetchall()
for t in existingTables:
    query = "SELECT * from " + t[0]
    df=conn.execute(query).fetchdf()
    dataframes[t[0]]=df
# print(len(dataframes))


# ### Applying deduplication of rows for each table

# In[6]:


dataframes.keys()


# In[7]:


for name,df in dataframes.items():
    #check if there are any duplicate rows
    indexs=df.duplicated(keep=False)
    x=np.where(indexs==True)[0]
    printmd("<br>")
    printmd('#### <font color=blue>Duplicated rows in {table}</font><br>'.format(table=name))
    #after checking we analyze repeted rows
    print(df.iloc[x].head())


# In[8]:


for name,df in dataframes.items():
    printmd('#### <font color=blue>Removing duplicated rows in {table}</font><br>'.format(table=name))
    print("Table dimension before deduplication:", df.shape )
    df.drop_duplicates(subset=None, keep="first", inplace=True)
    print("Table dimension after deduplication:", df.shape )


# ### Conexion to database

# In[9]:


def drop_table(conn, name):
        existingTables=conn.execute("SHOW TABLES").fetchall()
        if(len(existingTables)>0):
            for table in existingTables:
                if(table[0] == name):
                    conn.execute("DROP TABLE " + name)
def generate_table(conn, df, name):
        existingTables=conn.execute("SHOW TABLES").fetchall()
        if(len(existingTables)>0):
            for table in existingTables:
                if(table[0] == name):
                    conn.execute("DROP TABLE " + name)
        conn.execute("CREATE TABLE " + name + " AS SELECT * FROM df")


# In[10]:


for name,df in dataframes.items():
    drop_table(conn, name)
    generate_table(conn, df, name+"$v1")


# In[11]:


existingTables=conn.execute("SHOW TABLES").fetchall()


# In[12]:


existingTables


# In[13]:


conn.close()


#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import duckdb


# In[2]:


conn = duckdb.connect("../DB/DB_ExplotationZone", read_only=False)


# In[3]:


tb = []
existingTables=conn.execute("SHOW TABLES").fetchall()
if(len(existingTables)>0):
    for table in existingTables:
        tb.append(table[0])
print("Trustted Zone DB includes the following tables:\n ")
for t in tb:
    print( "------>", t, "\n")


# In[4]:


get_ipython().run_cell_magic('javascript', '', 'IPython.OutputArea.auto_scroll_threshold = 999999999;')


# In[5]:


from IPython.display import Markdown, display
def printmd(string):
    display(Markdown(string))


# In[6]:


existingTables=conn.execute("SHOW TABLES").fetchall()
if(len(existingTables)>0):
    for table in existingTables:
        query = "SELECT * from " + table[0]
        df = conn.execute(query.format()).fetchdf()
#         print("---------------------------------- " + table[0] + " ----------------------------------")
        printmd("<br>")
        printmd('### <font color=blue> Summary of dataset {table}</font><br>'.format(table=table[0]))
        display(df)


# In[7]:


conn.close()


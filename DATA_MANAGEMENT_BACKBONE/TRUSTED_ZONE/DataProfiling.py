#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
import seaborn as sns
import math
import os
from IPython.display import display
import duckdb
# !pip install skimpy
from skimpy import skim


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


import sys
dataframes={}
existingTables=conn.execute("SHOW TABLES").fetchall()
for table in existingTables:
    query = "SELECT * from " + table[0]
    df = conn.execute(query).fetchdf()
    dataframes[table[0]]=df


# In[6]:


conn.close()


# ### Generating dataset summary

# In[7]:


for name,df in dataframes.items():
    printmd("<br>")
    printmd('### <font color=blue> Summary of dataset {table}</font><br>'.format(table=name))
    skim(df)


# ### Data visualizations

# In[8]:


# !pip install dataprep
from dataprep.eda import *
from dataprep.eda import plot, plot_correlation, plot_missing, plot_diff, create_report


# #### Plots of dataset Startups

# In[9]:


plot(dataframes['startups_data'])


# #### Plots of dataset public schools

# In[10]:


plot(dataframes['Public_Schools'])


# ### Correlation summary

# In[11]:


plot_correlation(dataframes['startups_data'])


# In[12]:


plot_correlation(dataframes['Public_Schools'])


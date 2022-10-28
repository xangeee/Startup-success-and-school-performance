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
import datetime
from IPython.display import display
import duckdb
conn = duckdb.connect("../DB/DB_TrustedZone",read_only=False)
dataset1 = "Private_Schools"
dataset2 = "Public_Schools"


# In[2]:


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


# In[3]:


existingTables=conn.execute("SHOW TABLES").fetchall()
data_name = ""
for t in existingTables:
    if dataset1 in t[0]:
        data_name=t[0]
        query = "SELECT * from " + t[0]
        pri_df = conn.execute(query).fetchdf()
        break
        
# print(data_name)

data_name = ""
for t in existingTables:
    if dataset2 in t[0]:
        data_name=t[0]
        query = "SELECT * from " + t[0]
        pub_df = conn.execute(query).fetchdf()
        break
# print(data_name)


# # Data cleaning stage

# In[4]:


#we remove X and Y columns as it's not clear what they are and we don't need them
pub_df = pub_df.rename({'ï»¿X': 'X'}, axis=1) 
pri_df = pri_df.rename({'ï»¿X': 'X'}, axis=1) 
pub_df.drop(['X','Y'], axis=1, inplace=True)
pri_df.drop(['X','Y'], axis=1, inplace=True)


# In[5]:


# print("NAICS_CODE values:",pub_df['NAICS_CODE'].unique())
# print("NAICS_DESC values:",pub_df['NAICS_DESC'].unique())


# In[6]:


#we remove the columns NAICS_CODE and NAICS_DESC because theay have constant values
pub_df.drop(['NAICS_CODE','NAICS_DESC'], axis=1, inplace=True)
pri_df.drop(['NAICS_CODE','NAICS_DESC'], axis=1, inplace=True)


# In[7]:


pub_df['VAL_DATE'] = pub_df['VAL_DATE'].str.replace(':','').str.rstrip('0')
pub_df['YEAR'] = pd.DatetimeIndex(pub_df['VAL_DATE']).year

pri_df['VAL_DATE'] = pri_df['VAL_DATE'].str.replace(':','').str.rstrip('0')
pri_df['YEAR'] = pd.DatetimeIndex(pri_df['VAL_DATE']).year


# In[8]:


#same public school with two names different
pub_df = pub_df.replace({'NAME':'LINCOLN ELEMENTARY'},'LINCOLN ELEMENTARY SCHOOL')
pub_df.NAME.value_counts()


# #### Some information about dataset
# * ZIP has 5 digit zipcodes. ZIP4 is an additional 4-digit, and adds in a level of granularity in terms of geography.
# * TYPE column: what type of the school is.
# * STATUS column: what is the current operational status of school
# * Type labels:
#     1.Regular School
#     2.Special education school
#     3.Vocational school
#     4.Other/alternative school

# In[9]:


pub_df['TYPE'].value_counts()


# In[10]:


pri_df['TYPE'].value_counts()


# In[11]:


# Number of cities in each state 
pri_cities_state= pri_df[['STATE', 'CITY']].groupby('STATE').count().sort_values('CITY', ascending=False)
pub_cities_state= pub_df[['STATE', 'CITY']].groupby('STATE').count().sort_values('CITY', ascending=False)


# In[12]:


col = ['blue']
pub_cities_state.plot(kind = 'bar', legend=False, color=col, figsize=(10,5))
plt.show()


# In[13]:


col = ['red']
pri_cities_state.plot(kind = 'bar', legend=False, color=col, figsize=(10,5))
plt.show()


# ### Heatmaps

# In[14]:


import folium
from folium.plugins import HeatMap
#!pip install folium


# In[15]:


#PRIVATE SCHOOL MAP
aux_df = pri_df[['LATITUDE','LONGITUDE']]
aux_df.dropna(axis=0, subset=['LATITUDE','LONGITUDE'])
heat_data = [[row['LATITUDE'],row['LONGITUDE']] for index, row in aux_df.iterrows()]

pri_map = folium.Map([39.358, -98.118], zoom_start=5)
HeatMap(heat_data).add_to(pri_map)

pri_map


# In[16]:


#PUBLIC SCHOOL MAP
aux_df = pub_df[['LATITUDE','LONGITUDE']]
aux_df.dropna(axis=0, subset=['LATITUDE','LONGITUDE'])
heat_data2 = [[row['LATITUDE'],row['LONGITUDE']] for index, row in aux_df.iterrows()]

pub_map = folium.Map([39.358, -98.118], zoom_start=5)
HeatMap(heat_data2).add_to(pub_map)

pub_map


# ### Conexion to database

# In[17]:


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


# In[18]:


existingTables=conn.execute("SHOW TABLES").fetchall()
data_name = ""
for t in existingTables:
    if(dataset1 in t[0]):
        data_name = t[0]

drop_table(conn,data_name)
generate_table(conn,pri_df,dataset1)

data_name = ""
for t in existingTables:
    if(dataset2 in t[0]):
        data_name = t[0]

drop_table(conn,data_name)
generate_table(conn,pub_df,dataset2)


# In[19]:


# #show the table 
query = "SELECT * from " + dataset1
df=conn.execute(query.format()).fetchdf()
df.head()


# In[20]:


# #show the table 
query = "SELECT * from " + dataset2
df=conn.execute(query.format()).fetchdf()
df.head()


# In[21]:


conn.close()


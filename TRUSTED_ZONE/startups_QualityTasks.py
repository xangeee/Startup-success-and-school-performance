#!/usr/bin/env python
# coding: utf-8

# In[1]:


import duckdb
import pandas as pd
import os
from IPython.display import display
import datetime
conn = duckdb.connect("../DB/DB_TrustedZone",read_only=False)
dataset = "startups_data"


# In[2]:


existingTables=conn.execute("SHOW TABLES").fetchall()
data_name = ""
for t in existingTables:
    if(dataset in t[0]):
        data_name = t[0]
# print(data_name)
query = "SELECT * from " + data_name
df=conn.execute(query).fetchdf()


# ### Data cleaning 

# In[3]:


#Remove irrelevant columns
cols_to_remove = ["permalink", "homepage_url", "category_list","country_code"]
df = df.drop(cols_to_remove, axis=1)


# In[4]:


#check if there are any duplicate rows
df.duplicated().sum()


# In[5]:


#removing spaces on some column names
df.columns = df.columns.str.strip()


# In[6]:


# #Extracting year value from "first_funding_at" and changing to int
# df['first_funding_at'] = df.first_funding_at.str.split("-").str[0]
# df['first_funding_at'] = df['first_funding_at'].astype(int)

# #Extracting year value from "last_funding_at" and changing to int
# df['last_funding_at'] = df.last_funding_at.str.split("-").str[0]
# df['last_funding_at'] = df['last_funding_at'].astype(int)


# In[7]:


# #Changing the values in column "funding_total_usd" from string to float
# df['funding_total_usd'] = df['funding_total_usd'].str.strip().str.replace(",","")
# df['funding_total_usd'] = df['funding_total_usd'].replace("-",0).astype("float")


# In[8]:


# deleting the columns round_G and round_H because have zero values
df = df.drop(["round_G", "round_H"], axis=1)


# In[9]:


#Replacing missing status with "unknown"
#df['status'] = df['status'].replace(np.nan,"unknown")
#print(df.shape)
#df.head(40)


# In[10]:


from difflib import SequenceMatcher

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


# In[11]:


df['city'].unique()


# In[12]:


#missing values in the dataset
df.isnull().sum()


# In[13]:


# !pip install fuzzywuzzy
from fuzzywuzzy import fuzz

c_st = set(df["city"])

res = {}

for c1 in c_st:
    x = 0
    for c2 in c_st:
        if not isinstance(c1, float) and not isinstance(c2, float):
            if c1!= c2:
                if x < fuzz.ratio(c1, c2):
                    x = fuzz.ratio(c1, c2)
                    res[c1] = c2

res


# In[14]:


cities = [
#     'Stevenson Ranch',
    'Bloomfield',
    'Chicago Ridge',
    'Lees Summit',
    'Newport Beach',
    'El Cerrito',
#     'North Ridgeville',
#     'Centreville',
    'Deerfield Beach',
    'Trabuco Canyon',
    'Franklin Square',
#     'West Orange',
    'Port Richey',
    'North Charleston',
    'Lafayette',
    'Mc Lean',
#     'West Mansfield',
    'Ponte Vedra Beach',
    'East Palo Alto',
    'California',
    'Delaware City',
    'North Hollywood',
    'West Hartford',
    'East Sandwich',
    'Lewisville',
    'North Kansas City',
    'Newtown',
    'Petersburg',
    'West Chicago']

# cities = set([ str(city).upper() for city in cities ])
# df.city = [ str(city).upper() for city in df.city ]
# cities2 = ['La']


# In[15]:


for c in cities:
    x = c
    if len(c) > len(res[c]):
        x = res[c]
    df.city[df.city == c] = x


# In[16]:


df.city = [ str(city).upper() for city in df.city ]


# In[17]:


x = ["WEST ", "EAST", "NORTH", "SOUTH"] 
for city in df.city:
    for xx in x :
        if xx in city:
#             print(city)
            new_city = city.replace(xx, "")
            if new_city[0] == " ":
                new_city = new_city.replace(" ", "",1)
            df.city[df.city == city] = new_city


# In[18]:


df.city[df.city == "LA"] = "LOS ANGELES"


# In[19]:


any(df.city == "")


# ### Conexion to Database

# In[20]:


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


# In[21]:


drop_table(conn, dataset+"$v2")
generate_table(conn, df, dataset)


# In[22]:


# #show the table 
query = "SELECT * from " + dataset
df=conn.execute(query.format()).fetchdf()
df.head()


# In[23]:


conn.close()


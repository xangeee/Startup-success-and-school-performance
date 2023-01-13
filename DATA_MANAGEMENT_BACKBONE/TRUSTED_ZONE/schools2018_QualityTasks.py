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


# ### Reading table schools 2018 from database

# In[2]:


conn = duckdb.connect("../DB/DB_TrustedZone",read_only=False)
dataset = "schools2018"


# In[3]:


existingTables=conn.execute("SHOW TABLES").fetchall()
data_name = ""
for t in existingTables:
    if(dataset in t[0]):
        data_name = t[0]
# print(data_name)
query = "SELECT * from " + data_name
df=conn.execute(query).fetchdf()


# ### Data quality check

# In[4]:


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
df.head()


# In[5]:


# #Remove irrelevant columns
# columns=['ncessch','leaid','year','military_connected']
# df=df.drop(columns, axis=1)
# df.head()


# In[6]:


#!pip install great_expectations
import great_expectations as ge


# In[7]:


df_new = ge.from_pandas(df)


# In[8]:


#we expect the grade level of schools to be between 1 and 99.
df_new.expect_column_values_to_be_between(
    column="grade_edfacts", min_value=1, max_value=99
)


# In[9]:


#We also check if the values in a categorical column are in a given set
df_new.expect_column_values_to_be_in_set(
    column = "race", 
    value_set = list(range(1,10))+[20,99]
)

# race={"1":"White",
# "2":"Black",
# "3":"Hispanic",
# "4":"Asian",
# "5":"American Indian or Alaska Native",
# "6":"Native Hawaiian or other Pacific Islander",
# "7":"Two or more races",
# "8":"Nonresident alien",
# "9":"Unknown",
# "20":"Other",
# "99":"Total"}


# In[10]:


#We check if the values in a categorical column are in a given set
df_new.expect_column_values_to_be_in_set(
    column = "disability", 
    value_set = [0,1,2,3,4,99]
)
# disability={"0","Students without disabilities",
# "1","Students with disabilities served under IDEA",
# "2","Students with disabilities served under Section 504 only",
# "3","Students not served under IDEA (includes students without disabilities and students served under Section 504)",
# "4","Students with disabilities (served under Section 504 and under IDEA)",
# "99","Total"}


# In[11]:


#We check if the state codes are in a given set
df_new.expect_column_values_to_be_in_set(
    column = "fips", 
    value_set = list(range(1,96))
)


# In[12]:


#we select all numeric variables in order to check if there is any negative value
numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
onlyNumerics = df.select_dtypes(include=numerics)


# In[13]:


#the dataset contains negatives values but they are all special values, so there is no rare values
for column in onlyNumerics:
    uniqueValues=onlyNumerics[column].unique()
    if(uniqueValues<0).any().any()==True:
        print("* '{}' has the following negative values: ".format(column),uniqueValues[np.where(uniqueValues<0)])


# ### Conexion to Database

# In[14]:


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


# In[15]:


existingTables=conn.execute("SHOW TABLES").fetchall()
data_name = ""
for t in existingTables:
    if(dataset in t[0]):
        data_name = t[0]
        
drop_table(conn,data_name)
generate_table(conn,df,dataset)


# In[16]:


#show the table 
query = "SELECT * from " + dataset
df=conn.execute(query.format()).fetchdf()
df.head()


# In[17]:


conn.close()


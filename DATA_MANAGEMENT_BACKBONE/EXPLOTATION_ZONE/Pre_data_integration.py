#!/usr/bin/env python
# coding: utf-8

# In[1]:


import duckdb
import pandas as pd
import os
from IPython.display import display
import numpy as np


# In[2]:


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


# In[3]:


conn = duckdb.connect("../DB/DB_ExplotationZone",read_only=False)
existingTables=conn.execute("SHOW TABLES").fetchall()
existingTables


# In[4]:


#get data from DB
df_schools2018=conn.execute("SELECT * from schools2018".format()).fetchdf()
df_startups=conn.execute("SELECT * from startups_data".format()).fetchdf()
conn.close()


# ## Data quality processes for integration(SCHOOLS2018)

# In[5]:


df_schools2018.head(5)


# In[6]:


df=df_schools2018


# In[7]:


df=df[df['grade_edfacts']==99]
df_ori=df


# In[8]:


#firstly we select total number of tests results for all the columns that distinct between groups
df=df[df['race']==99]
df=df[df['sex']==99]
df=df[df['lep']==99]
df=df[df['homeless']==99]
df=df[df['migrant']==99]
df=df[df['disability']==99]
df=df[df['econ_disadvantaged']==99]
df=df[df['foster_care']==99]
df=df[df['military_connected']==99]


# In[9]:


#size of dataset before filtering,various rows for the same school
print("#schools in dataset orginal:",len(df_ori['ncessch_num'].unique()))
df_ori.shape


# In[10]:


#size of dataset before filtering, one row per each school
print("#schools in the new dataset:",len(df['ncessch_num'].unique()))
print(df.shape)
df.head()


# ### Declaration of auxiliar functions

# In[11]:


def getIndexs(newColumn):
    indices = np.where(np.in1d(df_ori['ncessch_num'].unique(), newColumn.unique()))[0]
    return indices

def createNewColumns(df,newColumn,columnName):
    indexs=getIndexs(newColumn['ncessch_num'])
    new_cols=np.zeros((df.shape[0],2),dtype=object)
    new_cols[indexs,0]=newColumn['read_test_num_valid']
    new_cols[indexs,1]=newColumn['math_test_num_valid']
    df["{name}_read_test_valid".format(name=columnName)]=new_cols[:,0]
    df["{name}_math_test_valid".format(name=columnName)]=new_cols[:,1]
    


# ### Removing unecessary  columns

# In[12]:


#Remove irrelevant columns
columns=['grade_edfacts','race','sex','lep','homeless','migrant','disability','econ_disadvantaged','foster_care','military_connected']
df=df.drop(columns, axis=1)
df.head()


# ### Converting Gender to columns

# In[13]:


men=df_ori[df_ori['sex']==1]
women=df_ori[df_ori['sex']==2]


# In[14]:


createNewColumns(df,men,"men")
createNewColumns(df,women,"women")
df.head()


# ### Converting Race to columns

# In[15]:


df_ori['race'].unique()
# 1—White
# 2—Black
# 3—Hispanic
# 4—Asian
# 5—American Indian or Alaska Native
# 6—Native Hawaiian or other Pacific Islander
# 7—Two or more races
# 8—Nonresident alien
# 9—Unknown
# 20—Other
# 99—Total


# In[16]:


white=df_ori[df_ori['race']==1]
black=df_ori[df_ori['race']==2]
hispanic=df_ori[df_ori['race']==3]
asian=df_ori[df_ori['race']==4]
native_indian=df_ori[df_ori['race']==5]
more2Races=df_ori[df_ori['race']==7]


# In[17]:


createNewColumns(df,white,"white")
createNewColumns(df,black,"black")
createNewColumns(df,hispanic,"hispanic")
createNewColumns(df,asian,"asian")
createNewColumns(df,native_indian,"native_indian")
createNewColumns(df,more2Races,"more2Races")

df.head()


# ### Converting lep(limited English proficient) to columns

# In[18]:


df_ori['lep'].unique()
#1—Students who are limited English proficient
#99—All students


# In[19]:


lepStudent=df_ori[df_ori['lep']==1]
createNewColumns(df,lepStudent,"lep")


# In[20]:


df.head()


# ### Converting migrant to columns

# In[21]:


df_ori['migrant'].unique()
#1—Students who are migrants
#99—All students


# In[22]:


migrant=df_ori[df_ori['migrant']==1]
createNewColumns(df,migrant,"migrant")
df.head()


# ### Converting disability to columns

# In[23]:


df_ori['disability'].unique()
# 0—Students without disabilities
# 1—Students with disabilities served under IDEA
# 2—Students with disabilities served under Section 504 only
# 3—Students not served under IDEA (includes students without disabilities and students served under Section 504)
# 4—Students with disabilities (served under Section 504 and under IDEA)
# 99—Total


# In[24]:


disability=df_ori[df_ori['disability']==1]
createNewColumns(df,disability,"disability")
df.head()


# ### Converting homeless to columns

# In[25]:


df_ori['homeless'].unique()
#1—Students who are homeless
#99—All students


# In[26]:


homeless=df_ori[df_ori['homeless']==1]
createNewColumns(df,homeless,"homeless")
df.head()


# ### Converting econ_disadvantaged to columns

# In[27]:


df_ori['econ_disadvantaged'].unique()
#1—Students who are economically disadvantaged
#99—All students


# In[28]:


econ_disadvantaged=df_ori[df_ori['econ_disadvantaged']==1]
createNewColumns(df,econ_disadvantaged,"econ_disadvantaged")
df.head()


# ### Converting foster_care to columns

# In[29]:


df_ori['foster_care'].unique()
#1—Students who are in foster care
#99—All students


# In[30]:


foster_care=df_ori[df_ori['foster_care']==1]
createNewColumns(df,foster_care,"foster_care")
df.head()


# ### Converting military_connected to columns

# In[31]:


df_ori['military_connected'].unique()
#1—Students who are connected to the military
#99—All students


# In[32]:


military_connected=df_ori[df_ori['military_connected']==1]
createNewColumns(df,military_connected,"military_connected")
df.head()


# In[33]:


df_schools=df


# ## Data quality processes for integration(STARTUPS)

# In[34]:


city=[str(city).upper() for city in df_startups['city']]


# In[35]:


df_startups['city']=city


# In[36]:


df_startups['city'].head()


# ### Conexion to Database

# In[37]:


conn = duckdb.connect("../DB/DB_ExplotationZone",read_only=False)
existingTables=conn.execute("SHOW TABLES").fetchall()
if(len(existingTables)>0):
    for table in existingTables:
        if(table[0]=='schools2018'):
            conn.execute("DROP TABLE schools2018")
            break
# create the table "my_table" from the DataFrame "my_df"
conn.execute("CREATE TABLE schools2018 AS SELECT * FROM df_schools")


# In[38]:


existingTables=conn.execute("SHOW TABLES").fetchall()
if(len(existingTables)>0):
    for table in existingTables:
        if(table[0]=='startups_data'):
            conn.execute("DROP TABLE startups_data")
            break
# create the table "my_table" from the DataFrame "my_df"
conn.execute("CREATE TABLE startups_data AS SELECT * FROM df_startups")


# In[39]:


#show the table 
df=conn.execute("SELECT * from schools2018".format()).fetchdf()
df.head()


# In[40]:


#show the table 
df=conn.execute("SELECT * from startups_data".format()).fetchdf()
df.head()


# In[41]:


conn.close()


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


# ### Declaration of auxiliar functions

# In[3]:


def getIndexs(column1,column2):
    indexs1=np.nonzero(np.in1d(column1,column2))[0]
    indexs2 = np.where(np.in1d(column2,column1))[0]
    return [indexs1,indexs2]


# ### Getting datasets to be joined

# In[4]:


conn = duckdb.connect("../DB/DB_ExplotationZone",read_only=False)
existingTables=conn.execute("SHOW TABLES").fetchall()
existingTables


# In[5]:


df_2018=conn.execute("SELECT * from schools2018".format()).fetchdf()
df_startups=conn.execute("SELECT * from startups_data".format()).fetchdf()


# In[6]:


#show the table 
df_pub=conn.execute("SELECT * from Public_Schools".format()).fetchdf()
df_pub.head()


# In[7]:


df_pri=conn.execute("SELECT * from Private_Schools".format()).fetchdf()
df_pri.head()


# In[8]:


conn.close()


# # Data integration

# In[9]:


df_2018=df_2018.sort_values('ncessch_num')
df_pub=df_pub.sort_values('NCESID')
df_pri=df_pri.sort_values('NCESID')


# In[10]:


schools2018=df_2018['ncessch_num'].unique()
schoolsPub=df_pub['NCESID'].unique()
schoolsPri=df_pri['NCESID'].unique()


# ### Joining tables "Schools2018" and "Public_schools"

# In[11]:


indexsList=getIndexs(schools2018,schoolsPub)


# In[12]:


new_df2018Pub=df_2018.iloc[indexsList[0],]
new_df2018Pub.head()


# In[13]:


new_dfPub=df_pub.iloc[indexsList[1],]
new_dfPub.head()


# In[14]:


# Add a multiple columns to the DataFrame
df_allPub =new_df2018Pub.assign(city=new_dfPub['CITY'],state=new_dfPub['STATE'],
                      enrollment=new_dfPub['ENROLLMENT'])


# In[15]:


df_allPub.head()


# ### Joining tables "Schools2018" and "Private_schools"

# In[16]:


schoolsPrivate=df_pri['NAME']
schools2018=dict()
for school in list(enumerate(df_2018['school_name'])):
    schools2018[school[1].upper()]=school[0]


# In[17]:


indSchools2018=[]
indSchoolsPrivate=[]
for school in list(enumerate(schoolsPrivate)):
    if school[1] in schools2018.keys():
        indSchools2018.append(schools2018[school[1]])
        indSchoolsPrivate.append(school[0])
        


# In[18]:


new_df2018Pri=df_2018.iloc[indSchools2018,]
new_df2018Pri.head()


# In[19]:


new_dfPri=df_pri.iloc[indSchoolsPrivate,]
new_dfPri.head()


# In[20]:


# Add a multiple columns to the DataFrame
df_allPri =new_df2018Pri.assign(city=list(new_dfPri['CITY']),state=list(new_dfPri['STATE']),
                      enrollment=list(new_dfPri['ENROLLMENT']))
df_allPri.head()


# ### Joining all the tables of schools: Schools2018, Public schools and Private schools

# ### We get a new table: Schools

# In[21]:


df_allPri.reset_index(inplace=True, drop=True)
df_allPub.reset_index(inplace=True, drop=True)
df_schools=pd.concat([df_allPub,df_allPri], axis=0)


# In[22]:


df_schools.head()


# ### Joining the tables "Startup" with "Schools"(new generated table)

# #### The first step is to group by all schools by CITY and sum the students performance of each school according to the city

# In[23]:


df_schoolsByCity=df_schools.groupby(['city']).sum()


# In[24]:


df_schoolsByCity.head()


# In[25]:


citiesSchools=list(df_schoolsByCity.index)
citieStartups=df_startups['city']


# In[26]:


indexsSchools=[]
indexsStartups=[]
for index,city in enumerate(citieStartups):
    if city in citiesSchools:
        indexsSchools.append(citiesSchools.index(city))
        indexsStartups.append(index)


# In[27]:


new_schools=df_schoolsByCity.iloc[indexsSchools]
new_startups=df_startups.iloc[indexsStartups]


# In[28]:


new_schools.head()


# In[29]:


new_startups.head()


# #### Now we can join the table Startups with Schools

# In[30]:


new_schools.reset_index(inplace=True, drop=True)
new_startups.reset_index(inplace=True, drop=True)
df_final=pd.concat([new_startups,new_schools], axis=1)


# In[31]:


df_final.head()


# ### Conexion to Database

# In[32]:


conn = duckdb.connect("../DB/DB_ExplotationZone",read_only=False)


# In[33]:


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


# In[34]:


generate_table(conn,df_final,"startups_studentsPerformance")


# In[35]:


#show the table 
df=conn.execute("SELECT * from startups_studentsPerformance").fetchdf()
df.head()


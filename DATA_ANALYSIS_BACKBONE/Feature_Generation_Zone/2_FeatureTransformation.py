#!/usr/bin/env python
# coding: utf-8

import duckdb
import pandas as pd
import os
from IPython.display import display
import numpy as np
from scipy import stats
 
# plotting modules
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


# ### Declaration of auxiliar functions

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


# ### Getting dataset

conn = duckdb.connect("../DB/DB_FeatureGeneration",read_only=False)
existingTables=conn.execute("SHOW TABLES").fetchall()

df=conn.execute("SELECT * from startups_studentsPerformance".format()).fetchdf()

#show the table 
df.head()

conn.close()


# # Data integration
df['status'].value_counts()

#funding total usd
df.columns = df.columns.str.strip()
df['funding_total_usd'] = df['funding_total_usd'].str.strip().str.replace(",","")
df['funding_total_usd'] = df['funding_total_usd'].replace("-",0).astype("float")
df['funding_total_usd'].describe()


# # **Transformations**

def drawPlots(originalData,fitted_data,fitted_lambda):
  # creating axes to draw plots
  fig, ax = plt.subplots(1, 2)
  
  # plotting the original data(non-normal) and
  # fitted data (normal)
  sns.distplot(originalData, hist = False, kde = True,
              kde_kws = {'shade': True, 'linewidth': 2},
              label = "Non-Normal", color ="green", ax = ax[0])
  
  sns.distplot(fitted_data, hist = False, kde = True,
              kde_kws = {'shade': True, 'linewidth': 2},
              label = "Normal", color ="green", ax = ax[1])
  
  # adding legends to the subplots
  plt.legend(loc = "upper right")
  
  # rescaling the subplots
  fig.set_figheight(5)
  fig.set_figwidth(10)
  
  print(f"Lambda value used for Transformation: {fitted_lambda}")


# **Transforming funding_total_usd**
# transform training data & save lambda value
fitted_data, fitted_lambda = stats.boxcox(df['funding_total_usd']+0.01)
df['transformed_funding_total_usd']=fitted_data
drawPlots(df['funding_total_usd']+0.01,fitted_data,fitted_lambda)


df.groupby('status')['transformed_funding_total_usd'].plot(kind='kde')


# **Transforming venture**
# transform training data & save lambda value
fitted_data, fitted_lambda = stats.boxcox(df['venture']+0.01)
df['transformed_venture']=fitted_data
drawPlots(df['venture']+0.01,fitted_data,fitted_lambda)

df.groupby('status')['transformed_venture'].plot(kind='kde')


# **Transforming debt_financing**

fitted_data, fitted_lambda = stats.boxcox(df['debt_financing']+0.01)
df['transformed_debt_financing']=fitted_data
drawPlots(df['debt_financing']+0.01,fitted_data,fitted_lambda)


df.groupby('status')['transformed_debt_financing'].plot(kind='kde')


# # **New Feafures Generation**
scrappedDate = datetime.strptime('2018-01-01', '%Y-%m-%d')

df['founded_at']=pd.to_datetime(df['founded_at'], errors = 'coerce')
df['first_funding_at']=pd.to_datetime(df['first_funding_at'], errors = 'coerce')
df['last_funding_at']=pd.to_datetime(df['last_funding_at'], errors = 'coerce')


df['nFoundedDays']=scrappedDate-df['founded_at']

df['nPrivateSchools']=df['nSchools']-df['nPublicSchools']

df['teachersPerStudent']=df['nTeachers'] / df['enrollment']

df['%noWhiteReadValid']=(df['read_test_num_valid']-df['white_read_test_valid'])/df['read_test_num_valid']
df['%noWhiteMathValid']=(df['math_test_num_valid']-df['white_math_test_valid'])/df['math_test_num_valid']

df['%EcoDisadReadValid']=df['econ_disadvantaged_read_test_valid']/df['read_test_num_valid']
df['%EcoDisadMathValid']=df['econ_disadvantaged_math_test_valid']/df['math_test_num_valid']

df['%asianReadValid']=df['asian_read_test_valid']/df['read_test_num_valid']
df['%asianMathValid']=df['asian_math_test_valid']/df['math_test_num_valid']


cols_delete = ['founded_at', 'ncessch_num', 'ncessch', 'leaid_num','year','leaid','fips',]
df = df.drop(cols_delete, axis=1)


corr = df.corr()
corr.style.background_gradient(cmap='coolwarm').set_precision(2)
# 'RdBu_r', 'BrBG_r', & PuOr_r are other good diverging colormaps


# # **Conexion to database**
conn = duckdb.connect("../DB/DB_FeatureGeneration",read_only=False)

generate_table(conn,df,"startups_studentsPerformance")


#show the table 
df=conn.execute("SELECT * from startups_studentsPerformance").fetchdf()
conn.close()


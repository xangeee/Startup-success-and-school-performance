#!/usr/bin/env python

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.impute import KNNImputer
from sklearn import preprocessing

conn = duckdb.connect("../DB/DB_FeatureGeneration", read_only=False)
existingTables = conn.execute("SHOW TABLES").fetchall()
existingTables
query = "SELECT * from startups_studentsPerformance"
df = conn.execute(query).fetchdf()


#remove columns with duplicated values in columns
df = df[df.iloc[:,1]!= df.iloc[:,2]]
df.head()

df = df.apply(pd.to_numeric, errors='ignore')
df.dtypes


df.describe()
df.status.value_counts()


cols = df.columns
num_cols = df._get_numeric_data().columns
cat_cols = list(set(cols) - set(num_cols))

# msno.bar(df)

# msno.heatmap(df)


# The value is 1. This means that there is a perfect correspondence between missing values in feature A and missing values in feature B. You can also see this from the matrix plot you made before.
# 
# The values in the heatmap range between -1 and 1. A value of -1 indicates a negative correspondence: A missing value in feature A implies that there is not a missing value in feature B.
# 
# Finally, a value of 0 indicates that there is no obvious correspondence between missing values in feature A and missing values in feature B. This is (more or less) the case for all the remaining features.

df[cat_cols] = df[cat_cols].fillna("Unknown")

# nan = np.nan
imputer = KNNImputer(n_neighbors=5, weights="uniform")
df[num_cols] = imputer.fit_transform(df[num_cols])


# msno.bar(df)


# # Normalization



d = preprocessing.normalize(df[num_cols])
df[num_cols] = d
df.head()


# *texto en cursiva*# Balance data


df.status.value_counts()
import numpy as np
df_op = df[df.status == "operating"]

df_op4 = df_op.sample(n=4000)
aux = set(df_op.index) - set(df_op4.index)
df = df.drop(aux)
df.status.value_counts()
df = df[df.status != "Unknown"]

X = df.loc[:, df.columns != "status"]
Y = df.loc[:, df.columns == "status"]


# #UNDERSAMPLING
# from imblearn.under_sampling import RandomUnderSampler
# under_sampler = RandomUnderSampler(random_state=42)
# X, Y = under_sampler.fit_resample(X, Y)
# Y.value_counts()

#OVERSAMPLING
from imblearn.over_sampling import RandomOverSampler
over_sampler = RandomOverSampler(random_state=42)
X, Y = over_sampler.fit_resample(X, Y)
Y.value_counts()


df = X
df['status'] = Y
df.status.value_counts()


#numeric columns
df[num_cols].describe()


#categorical columns
df[cat_cols].describe()
cols_delete = ["homepage_url", "name", "founded_quarter", "permalink", "country_code"]
df = df.drop(cols_delete, axis = 1)


df.describe()



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


generate_table(conn,df,"startups_studentsPerformance")
existingTables = conn.execute("SHOW TABLES").fetchall()
for t in existingTables:
    query = "SELECT * from " + t[0]
    df = conn.execute(query).fetchdf()
df.head()


conn.close()


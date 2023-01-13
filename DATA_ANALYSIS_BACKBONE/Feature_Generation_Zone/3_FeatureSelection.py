#!/usr/bin/env python
# coding: utf-8

import duckdb
import pandas as pd
import os
from IPython.display import display
import numpy as np


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
existingTables

df=conn.execute("SELECT * from startups_studentsPerformance".format()).fetchdf()

conn.close()


# # **PCA**
numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
x = df.select_dtypes(include=numerics)

df['target']=np.zeros(df['status'].size, dtype=int)

df['target'].iloc[df['status']=='operating']=0
df['target'].iloc[df['status']=='acquired']=1
df['target'].iloc[df['status']=='closed']=2


from sklearn.preprocessing import StandardScaler

# Separating out the target
y = df['target']

# # Standardizing the features
x = StandardScaler().fit_transform(x)

# # **PCA PROJECTION TO 2D**

from sklearn.decomposition import PCA

pca = PCA(0.70)
x_new = pca.fit_transform(x)
columnsName=['pc'+str(i+1) for i in range(x_new.shape[1])]

principalDf = pd.DataFrame(data = x_new, columns = columnsName)


#Concatenating DataFrame along axis = 1. finalDf is the final DataFrame before plotting the data.
finalDf = pd.concat([principalDf, df[['status']]], axis = 1)


finalDf.head()


# # **VISUALIZE 2D PROJECTION**
# plotting modules
import seaborn as sns
import matplotlib.pyplot as plt

fig, axes = plt.subplots(1,2)
axes[0].scatter(x[:,0], x[:,1],c=y)
axes[0].set_xlabel('x1')
axes[0].set_ylabel('x2')
axes[0].set_title('Before PCA')
axes[1].scatter(x_new[:,0], x_new[:,1],c=y)
axes[1].set_xlabel('PC1')
axes[1].set_ylabel('PC2')
axes[1].set_title('After PCA')
plt.show()


# plotting modules
import seaborn as sns
import matplotlib.pyplot as plt

fig = plt.figure(figsize = (8,8))
ax = fig.add_subplot(1,1,1) 
ax.set_xlabel('Principal Component 1', fontsize = 15)
ax.set_ylabel('Principal Component 2', fontsize = 15)
ax.set_title('2 component PCA', fontsize = 20)

targets = ['operating', 'acquired', 'closed']
colors = ['r', 'g', 'b']
for target, color in zip(targets,colors):
    indicesToKeep = finalDf['status'] == target
    ax.scatter(finalDf.loc[indicesToKeep, 'pc1']
               , finalDf.loc[indicesToKeep, 'pc2']
               , c = color
               , s = 50)
ax.legend(targets)
ax.grid()

pca.explained_variance_ratio_


# # **The biplot**

def biplot(score, coeff , y):
    '''
    Author: Serafeim Loukas, serafeim.loukas@epfl.ch
    Inputs:
       score: the projected data
       coeff: the eigenvectors (PCs)
       y: the class labels
   '''
    xs = score[:,0] # projection on PC1
    ys = score[:,1] # projection on PC2
    n = coeff.shape[0] # number of variables
    plt.figure(figsize=(10,8), dpi=100)
    classes = np.unique(y)
    colors = ['g','r','y']
    markers=['o','^','x']
    for s,l in enumerate(classes):
        plt.scatter(xs[y==l],ys[y==l], c = colors[s], marker=markers[s]) # color based on group
    for i in range(n):
        #plot as arrows the variable scores (each variable has a score for PC1 and one for PC2)
        plt.arrow(0, 0, coeff[i,0], coeff[i,1], color = 'k', alpha = 0.9,linestyle = '-',linewidth = 1.5, overhang=0.2)
        plt.text(coeff[i,0]* 1.15, coeff[i,1] * 1.15, "Var"+str(i+1), color = 'k', ha = 'center', va = 'center',fontsize=10)

    plt.xlabel("PC{}".format(1), size=14)
    plt.ylabel("PC{}".format(2), size=14)
    limx= int(xs.max()) + 1
    limy= int(ys.max()) + 1
    plt.xlim([-limx,limx])
    plt.ylim([-limy,limy])
    plt.grid()
    plt.tick_params(axis='both', which='both', labelsize=14)


import matplotlib as mpl
mpl.rcParams.update(mpl.rcParamsDefault) # reset ggplot style
# Call the biplot function for only the first 2 PCs
biplot(x_new[:,0:2], np.transpose(pca.components_[0:2, :]), y)
plt.show()


# # **Feature importance**


from sklearn.decomposition import PCA
import pandas as pd
import numpy as np
np.random.seed(0)

# number of components
n_pcs= pca.components_.shape[0]

columns=df.select_dtypes(include=numerics).columns


# get the index of the most important feature on EACH component
# LIST COMPREHENSION HERE
most_important = [np.abs(pca.components_[i]).argmax() for i in range(n_pcs)]

initial_feature_names = columns
# get the names
most_important_names = [initial_feature_names[most_important[i]] for i in range(n_pcs)]

# LIST COMPREHENSION HERE AGAIN
dic = {'PC{}'.format(i): most_important_names[i] for i in range(n_pcs)}

# build the dataframe
importantFeatures = pd.DataFrame(dic.items())
importantFeatures.head()


# We look at the absolute values of the eigenvectors’ components corresponding to the k largest eigenvalues. In sklearn the components are sorted by explained variance. The larger they are these absolute values, the more a specific feature contributes to that principal component.

absEigenv=abs( pca.components_ )


pc1MostImportantFeaturesIndex=np.argpartition(absEigenv[0], -8)[-8:]
pc2MostImportantFeaturesIndex=np.argpartition(absEigenv[1], -8)[-8:]


columns[pc1MostImportantFeaturesIndex]

columns[pc2MostImportantFeaturesIndex]

finalDf.head()

x=finalDf.iloc[:, 0:6]
y=finalDf.iloc[:,-1]

x.head()
y.head()


# # **Conexion to Database**
conn = duckdb.connect("../DB/DB_FeatureGeneration",read_only=False)


df_percent = df.sample(frac=0.95)
df_rest = df.loc[~df.index.isin(df_percent.index)]
pcaForm = finalDf.loc[~finalDf.index.isin(df_percent.index)]
df_rest.to_csv("../DEPLOYMENT/MODEL/testing_data.csv", encoding='utf-8', index=False)
pcaForm.to_csv("../DEPLOYMENT/MODEL/testing_data_pca.csv", encoding='utf-8', index=False)


generate_table(conn,finalDf,"dataForModelling")
existingTables=conn.execute("SHOW TABLES").fetchall()
existingTables

#show the table 
df=conn.execute("SELECT * from dataForModelling").fetchdf()

conn.close()


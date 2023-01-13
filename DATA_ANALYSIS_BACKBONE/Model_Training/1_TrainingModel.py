import duckdb
import pandas as pd
import os
from IPython.display import display
import numpy as np
import random

random.seed(10)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

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


conn = duckdb.connect("../DB/DB_Train&Test",read_only=False)
existingTables=conn.execute("SHOW TABLES").fetchall()
existingTables

train=conn.execute("SELECT * from train_data".format()).fetchdf()
test=conn.execute("SELECT * from test_data".format()).fetchdf()

#show the table 
train.head()

test.head()

X_train = train.iloc[:, train.columns != "status"]
y_train = train.status
X_test = test.iloc[:, test.columns != "status"]
y_test = test.status

conn.close()

# Load libraries
import pandas as pd
from sklearn.tree import DecisionTreeClassifier # Import Decision Tree Classifier
from sklearn.model_selection import train_test_split # Import train_test_split function
from sklearn import metrics #Import scikit-learn metrics module for accuracy calculation
from sklearn import decomposition, datasets
from sklearn import tree
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV
from sklearn.preprocessing import StandardScaler

dec_tree = tree.DecisionTreeClassifier()

dec_tree.get_params().keys()

pipe = Pipeline(steps=[('dec_tree', dec_tree)])

criterion = ['gini', 'entropy']
max_depth = [6,8,10,12,20,30,40]
min_samples_splits = [0.2,0.4,0.6,0.8,1.]#min_samples_split represents the minimum number of samples required to split an internal node
min_samples_leafs = [0.1, 0.2, 0.3, 0.4, 0.5]#The minimum number of samples required to be at a leaf node. 
#max_features = list(range(1,X_train.shape[1]))#max_features represents the number of features to consider when looking for the best split.

parameters = dict(dec_tree__criterion=criterion,
                  dec_tree__max_depth=max_depth,
                  dec_tree__min_samples_split=min_samples_splits,
                  dec_tree__min_samples_leaf=min_samples_leafs,
                  #dec_tree__max_features=max_features
                  )


parameters

clf_GS = GridSearchCV(pipe, parameters)
clf_GS.fit(X_train, y_train)

print('Best criterion:', clf_GS.best_estimator_.get_params()['dec_tree__criterion'])
print('Best max_depth:', clf_GS.best_estimator_.get_params()['dec_tree__max_depth'])
print('Best min_samples_split:', clf_GS.best_estimator_.get_params()['dec_tree__min_samples_split'])
print('Best min_samples_leaf:', clf_GS.best_estimator_.get_params()['dec_tree__min_samples_leaf'])
#print('Best max_features:', clf_GS.best_estimator_.get_params()['dec_tree_max__features'])

print(); print(clf_GS.best_estimator_.get_params()['dec_tree'])

p_criterion=clf_GS.best_estimator_.get_params()['dec_tree__criterion']
p_max_depth=clf_GS.best_estimator_.get_params()['dec_tree__max_depth']
p_min_samples_split=clf_GS.best_estimator_.get_params()['dec_tree__min_samples_split']
p_min_samples_leaf=clf_GS.best_estimator_.get_params()['dec_tree__min_samples_leaf']


# Create Decision Tree classifer object
clf = DecisionTreeClassifier(criterion=p_criterion, max_depth=20)

# Train Decision Tree Classifer
clf = clf.fit(X_train,y_train)

#Predict the response for test dataset
y_pred = clf.predict(X_test)


# Model Accuracy, how often is the classifier correct?
print("Accuracy:",metrics.accuracy_score(y_test, y_pred))


import matplotlib.pyplot as plt
#plt.figure(figsize=(12,8))
#tree.plot_tree(clf, filled=True, fontsize=10)
#plt.show()

get_ipython().system('pip install graphviz')


get_ipython().system('pip install pydotplus')


from sklearn.tree import export_graphviz
from six import StringIO
from IPython.display import Image  
import pydotplus

#dot_data = StringIO()
#export_graphviz(clf, out_file=dot_data,  
                #  filled=True, rounded=True,
                #  special_characters=True)
#graph = pydotplus.graph_from_dot_data(dot_data.getvalue())  
#graph.write_png('tree.png')
#Image(graph.create_png())
 
import pickle
pickle.dump(clf, open('../../DEPLOYMENT/MODEL/model.pkl', 'wb'))


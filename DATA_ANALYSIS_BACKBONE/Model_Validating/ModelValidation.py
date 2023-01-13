import duckdb
import pandas as pd
import os
import numpy as np
from sklearn import metrics #Import scikit-learn metrics module for accuracy calculation
import pickle



conn = duckdb.connect("../DB/DB_Train&Test", read_only=False)
existingTables=conn.execute("SHOW TABLES").fetchall()
existingTables
test=conn.execute("SELECT * from test_data".format()).fetchdf()
train=conn.execute("SELECT * from train_data".format()).fetchdf()


X_test = test.iloc[:, test.columns!="status"]
y_test = test.status

X_train = train.iloc[:, train.columns!="status"]
y_train = train.status


from sklearn.metrics import f1_score

clf = pickle.load(open('../../DEPLOYMENT/MODEL/model.pkl', 'rb'))

y_pred = clf.predict(X_test)

# Model Accuracy, how often is the classifier correct?
print("Accuracy:", round(metrics.accuracy_score(y_test, y_pred),4))
print("Recall:", round(metrics.recall_score(y_test, y_pred, average= "weighted"),4))
print("Precision:", round(metrics.precision_score(y_test, y_pred, average= "weighted"),4))
print("F1-score:", round(metrics.f1_score(y_test, y_pred, average= "weighted"),4))

y_test.value_counts

frames = [train, test]
df = pd.concat(frames)
X = df.iloc[:, df.columns!="status"]
y = df.status

from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import KFold 
from sklearn.metrics import accuracy_score
from sklearn.model_selection import cross_val_score

# Implementing cross validation
 
k = 5
kf = KFold(n_splits=k, random_state=None)
acc_score = []
 
model = DecisionTreeClassifier(criterion="entropy", max_depth=20)
result = cross_val_score(model , X, y, cv = kf)
print("Accuracy of each fold:", result)
print("Avg accuracy: {}".format(result.mean()))

import os
os.system('jupyter nbconvert --to html ../DATA ANALYSIS/MODEL VALIDING/ModelVisualization.ipynb')

conn.close() 


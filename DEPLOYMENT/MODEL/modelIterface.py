# -*- coding: utf-8 -*-
"""
Created on Thu Jan 12 02:44:32 2023

@author: xange
"""
from  tkinter import *
from  tkinter import ttk
import pickle
import csv
import pandas as pd
import sys
from sklearn.decomposition import PCA
import random


model = pickle.load(open('model.pkl', 'rb'))
testing_data = pd.read_csv('testing_data.csv')

testing_data_pca = pd.read_csv('testing_data_pca.csv')

columns=['market', 'funding_total_usd','city','founded_month','nFoundedDays','first_funding_at','last_funding_at','grant','venture','nSchools','nTeachers','nPublicSchools','teachersPerStudent','read_test_num_valid','math_test_num_valid','asian_math_test_valid','%noWhiteMathValid','military_connected_math_test_valid','%EcoDisadMathValid']
data_to_display=testing_data[columns]


ws  = Tk()
ws.title('CheckItOut')
ws.geometry('1400x500')
ws['bg'] = '#AC99F2'

game_frame = Frame(ws)
game_frame.pack()

#scrollbar
game_scroll = Scrollbar(game_frame)
game_scroll.pack(side=RIGHT, fill=Y)

game_scroll = Scrollbar(game_frame,orient='horizontal')
game_scroll.pack(side= BOTTOM,fill=X)

my_game = ttk.Treeview(game_frame,yscrollcommand=game_scroll.set, xscrollcommand =game_scroll.set)


my_game.pack()

game_scroll.config(command=my_game.yview)
game_scroll.config(command=my_game.xview)

#define our column
tupleColumns=tuple(["col_{name}".format(name=i)for i in columns])
#my_game['columns'] = ('player_Name', 'player_Country', 'player_Medal','player_Medalx','player_Medaly','player_Medalz','player_Medalw')
my_game['columns']= tupleColumns


# format our column
my_game.column("#0", width=0,  stretch=NO)
for col_tup in tupleColumns:
    my_game.column("{name}".format(name=col_tup),anchor=CENTER, width=100)

#Create Headings 
my_game.heading("#0",text="",anchor=CENTER)
for col,col_tup in zip(columns,tupleColumns):
    my_game.heading("{col_tup}".format(col_tup=col_tup),text="{col_name}".format(col_name=col),anchor=CENTER)

    

row_list =[data_to_display.loc[i, :].values.flatten().tolist() for i in range(0,50)]
testing_data_pca_display=[testing_data_pca.loc[i, :].values.flatten().tolist() for i in range(0,50)]

#add data 
for index,elem in enumerate(row_list):
    valuesString=tuple([str(i) for i in elem])
    my_game.insert(parent='',index='end',iid=index,text='',
    values=valuesString)



my_game.pack()

frame = Frame(ws)
frame.pack(pady=20)

columns=['market', 'funding_total_usd','city','founded_month','nFoundedDays','first_funding_at','last_funding_at','grant','venture','nSchools','nTeachers','nPublicSchools','teachersPerStudent','read_test_num_valid','math_test_num_valid','asian_math_test_valid','%noWhiteMathValid','military_connected_math_test_valid','%EcoDisadMathValid']

#labels
market= Label(frame,text = "Market")
market.grid(row=0,column=0 )

funding_total_usd = Label(frame,text="funding_total_usd")
funding_total_usd.grid(row=0,column=1)

city = Label(frame,text="city")
city.grid(row=0,column=2)

founded_month = Label(frame,text="founded_month")
founded_month.grid(row=0,column=3)

nFoundedDays= Label(frame,text = "nFoundedDays")
nFoundedDays.grid(row=0,column=4 )

playername = Label(frame,text="first_funding_at")
playername.grid(row=0,column=5)

last_funding_at = Label(frame,text="last_funding_at")
last_funding_at.grid(row=0,column=6)

grant = Label(frame,text="grant")
grant.grid(row=0,column=7)

venture = Label(frame,text="venture")
venture.grid(row=0,column=8)

nSchools= Label(frame,text = "nSchools")
nSchools.grid(row=0,column=9 )

nTeachers = Label(frame,text="nTeachers")
nTeachers.grid(row=0,column=10)

nPublicSchools = Label(frame,text="nPublicSchools")
nPublicSchools.grid(row=0,column=11)

teachersPerStudent = Label(frame,text="teachersPerStudent")
teachersPerStudent.grid(row=0,column=12)

read_test_num_valid = Label(frame,text="read_test_num_valid")
read_test_num_valid.grid(row=0,column=13)

math_test_num_valid= Label(frame,text = "math_test_num_valid")
math_test_num_valid.grid(row=0,column=14 )

asian_math_test_valid = Label(frame,text="asian_math_test_valid")
asian_math_test_valid.grid(row=0,column=15)

noWhiteMathValid = Label(frame,text="%noWhiteMathValid")
noWhiteMathValid.grid(row=0,column=16)

military_connected_math_test_valid = Label(frame,text="military_connected_math_test_valid")
military_connected_math_test_valid.grid(row=0,column=17)

EcoDisadMathValid= Label(frame,text = "%EcoDisadMathValid")
EcoDisadMathValid.grid(row=0,column=18)


#Entry boxes
market_entry= Entry(frame)
market_entry.grid(row= 1, column=0)

funding_total_usd_entry = Entry(frame)
funding_total_usd_entry.grid(row=1,column=1)

city_entry = Entry(frame)
city_entry.grid(row=1,column=2)

founded_month_entry = Entry(frame)
founded_month_entry.grid(row=1,column=3)

nFoundedDays_entry = Entry(frame)
nFoundedDays_entry.grid(row=1,column=4)

first_funding_at_entry = Entry(frame)
first_funding_at_entry.grid(row=1,column=5)

last_funding_at_entry = Entry(frame)
last_funding_at_entry.grid(row=1,column=6)

grant_entry = Entry(frame)
grant_entry.grid(row=1,column=7)

venture_entry = Entry(frame)
venture_entry.grid(row=1,column=8)

nSchools_entry = Entry(frame)
nSchools_entry.grid(row=1,column=9)


nTeachers_entry = Entry(frame)
nTeachers_entry.grid(row=1,column=10)

nPublicSchools_entry = Entry(frame)
nPublicSchools_entry.grid(row=1,column=1)

teachersPerStudent_entry = Entry(frame)
teachersPerStudent_entry.grid(row=1,column=12)


read_test_num_valid_entry = Entry(frame)
read_test_num_valid_entry.grid(row=1,column=13)

math_test_num_valid_entry = Entry(frame)
math_test_num_valid_entry.grid(row=1,column=14)

asian_math_test_valid_entry = Entry(frame)
asian_math_test_valid_entry.grid(row=1,column=15)


noWhiteMathValid_entry = Entry(frame)
noWhiteMathValid_entry.grid(row=1,column=16)

military_connected_math_test_valid_entry = Entry(frame)
military_connected_math_test_valid_entry.grid(row=1,column=17)

EcoDisadMathValid_entry = Entry(frame)
EcoDisadMathValid_entry.grid(row=1,column=18)

selectedIndex=0
predicted_status=""
editedItems=[]
#Select Record
def select_record():
    #clear entry boxes
    market_entry.delete(0,END)

    funding_total_usd_entry.delete(0,END)

    city_entry.delete(0,END)

    founded_month_entry.delete(0,END)

    nFoundedDays_entry.delete(0,END)

    first_funding_at_entry.delete(0,END)

    last_funding_at_entry.delete(0,END)

    grant_entry.delete(0,END)

    venture_entry.delete(0,END)

    nSchools_entry.delete(0,END)

    nTeachers_entry.delete(0,END)

    nPublicSchools_entry.delete(0,END)

    teachersPerStudent_entry.delete(0,END)

    read_test_num_valid_entry.delete(0,END)

    math_test_num_valid_entry.delete(0,END)

    asian_math_test_valid_entry.delete(0,END)

    noWhiteMathValid_entry.delete(0,END)

    military_connected_math_test_valid_entry.delete(0,END)

    EcoDisadMathValid_entry.delete(0,END)
    
    #grab record
    selected=my_game.focus()
    #grab record values
    values = my_game.item(selected,'values')
    #temp_label.config(text=selected)
    print("selected:",values)
    selectedIndex=my_game.index(my_game.selection())
    print("selected index:",selectedIndex)
    print(editedItems)
    temp_label.config(text="")

    tatget_label.config(text="")
    if(selectedIndex not in editedItems):
        target=testing_data_pca_display[selectedIndex][-1]
        test_PCA=[testing_data_pca_display[selectedIndex][:-1]]
        
        #Predict the response for test dataset
        predicted_status = model.predict(test_PCA)
        #print("pred:",y_pred)
        #print(target)
        temp_label.config(text="The predicted class for the individual is : {name}".format(name=predicted_status[0]))
    
        tatget_label.config(text="The actual class for the individual is : {name}".format(name=target))
       
    else:
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        #x=pd.DataFrame (list(values), columns = columns )
        #x = df.select_dtypes(include=numerics)
        # # Standardizing the features
       # x = StandardScaler().fit_transform(x)
        #pca = PCA(0.70)
        #x_new = pca.fit_transform(x)
        #columnsName=['pc'+str(i+1) for i in range(x_new.shape[1])]
        
        #principalDf = pd.DataFrame(data = x_new, columns = columnsName)
        #Predict the response for test dataset
        #predicted_status = model.predict(test_PCA)
        selectedIndex=random.randint(0, 49)
        target=testing_data_pca_display[selectedIndex][-1]
        test_PCA=[testing_data_pca_display[selectedIndex][:-1]]
        
        #Predict the response for test dataset
        predicted_status = model.predict(test_PCA)
        temp_label.config(text="The predicted class for the individual is : {name}".format(name=predicted_status[0]))
        
    #output to entry boxes
    market_entry.insert(0,values[0])

    funding_total_usd_entry.insert(0,values[1])

    city_entry.insert(0,values[2])

    founded_month_entry.insert(0,values[3])

    nFoundedDays_entry.insert(0,values[4])

    first_funding_at_entry.insert(0,values[5])

    last_funding_at_entry.insert(0,values[6])

    grant_entry.insert(0,values[7])

    venture_entry.insert(0,values[8])

    nSchools_entry.insert(0,values[9])

    nTeachers_entry.insert(0,values[10])

    nPublicSchools_entry.insert(0,values[11])
    
    teachersPerStudent_entry.insert(0,values[12])

    read_test_num_valid_entry.insert(0,values[13])

    math_test_num_valid_entry.insert(0,values[14])

    asian_math_test_valid_entry.insert(0,values[15])

    noWhiteMathValid_entry.insert(0,values[16])

    military_connected_math_test_valid_entry.insert(0,values[17])

    EcoDisadMathValid_entry.insert(0,values[18])


#save Record
def update_record():
    selected=my_game.focus()
    #save new data 
    my_game.item(selected,text="",values=(market_entry.get(),funding_total_usd_entry.get(),
                                          city_entry.get(),founded_month_entry.get(),
                                          nFoundedDays_entry.get(),first_funding_at_entry.get(),
                                          last_funding_at_entry.get(),grant_entry.get(),
                                          venture_entry.get(),nSchools_entry.get(),
                                          nTeachers_entry.get(),nPublicSchools_entry.get(),
                                          teachersPerStudent_entry.get(),

                                          read_test_num_valid_entry.get(),

                                          math_test_num_valid_entry.get(),

                                          asian_math_test_valid_entry.get(),

                                          noWhiteMathValid_entry.get(),

                                          military_connected_math_test_valid_entry.get(),

                                          EcoDisadMathValid_entry.get()
                                          ))
    selectedIndex=my_game.index(my_game.selection())
    editedItems.append(selectedIndex)
   
   #clear entry boxes
"""
    market_entry.delete(0,END)

    funding_total_usd_entry.delete(0,END)

    city_entry.delete(0,END)

    founded_month_entry.delete(0,END)

    nFoundedDays_entry.delete(0,END)

    first_funding_at_entry.delete(0,END)

    last_funding_at_entry.delete(0,END)

    grant_entry.delete(0,END)

    venture_entry.delete(0,END)

    nSchools_entry.delete(0,END)

    nTeachers_entry.delete(0,END)

    nPublicSchools_entry.delete(0,END)

    teachersPerStudent_entry.delete(0,END)

    read_test_num_valid_entry.delete(0,END)

    math_test_num_valid_entry.delete(0,END)

    asian_math_test_valid_entry.delete(0,END)

    noWhiteMathValid_entry.delete(0,END)

    military_connected_math_test_valid_entry.delete(0,END)

    EcoDisadMathValid_entry.delete(0,END)
   
"""
#Buttons
select_button = Button(ws,text="Select the individual to predict", command=select_record)
select_button.pack(pady =10)

edit_button = Button(ws,text="Edit ",command=update_record)
edit_button.pack(pady = 10)

temp_label =Label(ws,text="")
temp_label.pack()

space =Label(ws,text="")
space.pack()

tatget_label =Label(ws,text="")
tatget_label.pack()
ws.mainloop()

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.linear_model import  LinearRegression
from sklearn.model_selection import train_test_split
import pickle


dataframe =  pd.read_csv("myproject/data/data.csv")
dataframe.dropna(inplace=True)
X=dataframe[['Rooms','Landsize','BuildingArea']]
y=dataframe['Price']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4)

lm=LinearRegression()
lm.fit(X_train,y_train)

pickle.dump(lm, open('model.pkl','wb'))

model = pickle.load(open('model.pkl','rb'))
print(model.predict([[4, 300, 500]]))


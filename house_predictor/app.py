import numpy as np
from flask import Flask, request, jsonify, render_template
import pickle
import logging
import pandas as pd

app = Flask(__name__)
model = pickle.load(open('model.pkl', 'rb'))

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/predict',methods=['POST'])
def predict():

    
    rooms=request.form['Rooms']
    landsize=request.form['Landsize']
    buildingArea=request.form['BuildingArea']

    input_features = pd.DataFrame([[rooms, landsize, buildingArea]],
                                       columns=['rooms', 'landsize', 'buildingArea'],
                                       dtype=float)
    prediction = model.predict(input_features)[0]

    output = round(prediction, 2)

    return render_template('index.html', prediction_text='$ {}'.format(output),original_input={'Rooms':rooms, 'Landsize':landsize, 'BuildingArea':buildingArea})

@app.route('/results',methods=['POST'])
def results():

    data = request.get_json(force=True)
    prediction = model.predict([np.array(list(data.values()))])

    output = prediction[0]
    return jsonify(output)

if __name__ == "__main__":
    app.run(debug=True)
import json
from flask import Flask, render_template, jsonify
import pandas as pd
import numpy as np
import networkx as nx
import community
import functions as fn
import yaml
import listener
#import parameters
import codecs
import os

with open('parameters.yaml') as file:
    parameters = yaml.full_load(file)
lccs = parameters['connected_components']
k_cores = parameters['k_cores']
project = parameters['project']

application = Flask(__name__)

# 2. Declare data stores
class DataStore():
    foo = None

data = DataStore()

@application.route("/", methods=["GET", "POST"])
def index():
    return render_template("index.html")


@application.route("/get-data", methods=["GET", "POST"])
def returnProdData():
    out_path = 'data/' + str(project) + '/preprocessed/'
    file = sorted(os.listdir(out_path))[0]
    #f = pd.read_json(codecs.open(str(out_path) + str(file), 'r', 'utf-8'))
    with open(str(out_path) + str(file)) as json_file:
        f = json.load(json_file)
    return jsonify(f)

if __name__ == "__main__":
    application.run(debug=False)

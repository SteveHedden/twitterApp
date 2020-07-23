import pandas as pd
import numpy as np
import os
import json
from flask import jsonify
import codecs
from collections import Counter
import networkx as nx
from community import community_louvain
import itertools
import community
import datetime
import yaml
import functions as fn
import NLP as NLP

with open('parameters.yaml') as file:
    parameters = yaml.full_load(file)
project = parameters['project']


condensed_path = 'data/' + str(project) + '/CONDENSED/'
if not os.path.exists(condensed_path):
    os.makedirs(condensed_path)

communities_path = 'data/' + str(parameters['project']) + '/communities/'

# Get subset of X latest raw files excluding the last
file_list = sorted(os.listdir(communities_path))
sub_list = file_list[:]

# Concatinate raw files
raw = []
for filename in sub_list:
    try:
        f = open(communities_path + filename, 'r')
        ds = json.load(f)
        raw.extend(ds)
        f.close()
    except:
        print('Error in: ' + str(filename))


raw = pd.DataFrame(raw)
#raw.to_csv("test.csv")
#raw['number_of_retweets'] = raw['number_of_retweets'].map({'nan': 0})
raw['number_of_retweets'] = raw['number_of_retweets'].replace({'nan': 0})
raw['number_of_retweets'] = raw['number_of_retweets'].astype(int)

#raw.to_csv("test2.csv")
#raw = fn.cleanRawData(raw)


#raw['date'] = raw['date'].apply(lambda x: x.strftime('%H:%M:%S.%f - %b %d %Y'))

#Need unique groups for each community and timestep
raw['unique'] = raw['date'].astype(str) +  " " + raw['group'].astype(str)

#raw.to_csv("test2.csv")
groups = raw["unique"].unique().tolist()

#Create hypergraph - each member of a group gets a connection to each other member
df = pd.DataFrame()
counter = 0
for x in groups:
    counter = counter + 1
    group = raw.loc[raw['unique'] == x]
    temp = pd.DataFrame(list(itertools.combinations(group['influencers'], 2)))
    if len(temp) == 0:
        continue
    temp.columns= ['source', 'target']
    df = df.append(temp)

df = df.groupby(["source", "target"]).size().reset_index(name="freq")

#raw.to_csv("test3.csv")


raw['rt_count'] = raw['number_of_retweets']
raw['original_screen_name'] = raw['influencers']
#raw['id_str'] = raw['tweet_id']
raw = raw.rename(columns={'tweet_id': 'id_str'})
raw['text'] = raw['tweet_text']

#Create graph using the data
G=nx.from_pandas_edgelist(df, 'source', 'target',edge_attr='freq')
#GU=nx.from_pandas_edgelist(df, 'source', 'target')

# Filter graph
G = fn.filter_for_k_core(G, k_cores=5)
G = fn.filter_for_largest_components(G, num_comp=20)

# Communities and centralities
partition = community.best_partition(G)
dc = nx.degree_centrality(G)

#Get node df for ALL tweets (for NLP)
nodes = fn.getNodes(G, dc, partition, raw)

# Write to master df
#fn.buildCommunityData(nodes, topGroups, df)
#raw = fn.cleanRawData(raw)
NLP.NLP(nodes, raw)

# Write edgelist to dataframe
edges = G.edges()
edges = pd.DataFrame(edges)
edges.columns = ['source', 'target']

# Update edgelists to use ids (necessisary?)
edges['source'] = edges['source'].apply(lambda x: nodes.loc[nodes['name'] == x]['id'].values[0])
edges['target'] = edges['target'].apply(lambda x: nodes.loc[nodes['name'] == x]['id'].values[0])

# Convert nodes and edges to dictionaries
node_dict = nodes.to_dict(orient='records')
edge_dict = edges.to_dict(orient='records')
graph_dict = {'nodes': node_dict, 'links': edge_dict}  # Because they are called links in main.js

# Write to json with datetime stamp
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
f = open(str(condensed_path) + 'CONDENSEDs.json', 'w')  # MacOS path
json.dump(graph_dict, f)  # dump the tweets into a json file
f.close()
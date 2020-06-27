
# For concatination and file managment
import datetime
import json
import yaml
import os

# For network building
import pandas as pd
import numpy as np
import networkx as nx
import community
import functions as fn
import codecs


# Load parameters
with open('parameters.yaml') as file:
    parameters = yaml.full_load(file)
project = parameters['project']
raw_batch_size = parameters['raw_batch_size']
lccs = parameters['connected_components']
k_cores = parameters['k_cores']

# Concatinate
raw_out_path = 'data/' + str(project) + '/raw_out/'
out_path = 'data/' + str(project) + '/preprocessed/'

# Get subset of X latest raw files excluding the last
file_list = sorted(os.listdir(raw_out_path))
sub_list = file_list[-raw_batch_size - 1:-1]

# Concatinate raw files
tweet_list = []
for filename in sub_list:
    try:
        f = open(raw_out_path + filename,'r')
        ds = json.load(f)
        tweet_list.extend(ds)
        f.close()
    except:
        print('Error in: ' + str(filename))

# Process data into network for application.py
#file = str(out_path) + str(sorted(os.listdir(out_path))[0])

#df = pd.read_json(codecs.open(file, 'r', 'utf-8'))

df = pd.DataFrame(tweet_list)

# Create a source column in the df for the sn of the tweeter
df['source'] = df['user'].apply(lambda x: fn.getScreenName(x))
df['target'] = df['retweeted_status'].apply(lambda x: fn.getOrigScreenName(x))

# Break into retweets and non-reweets
rts = df.loc[~pd.isnull(df['retweeted_status'])]
nonrts = df.loc[pd.isnull(df['retweeted_status'])]

# Create edges for rts
edges1 = pd.DataFrame()  # Create empty df
edges1['source'] = rts['user'].apply(lambda x: fn.getScreenName(x))
edges1['target'] = rts['retweeted_status'].apply(lambda x: fn.getOrigScreenName(x))

# Create edges for mentions
edges2 = pd.DataFrame()
edges2['source'] = nonrts['user'].apply(lambda x: fn.getScreenName(x))
edges2['target'] = nonrts['entities'].apply(lambda x: fn.getMentions(x))
edges2 = edges2.dropna()
edges = pd.concat([edges1, edges2])

# Create graph using the data
G = nx.from_pandas_edgelist(edges, 'source', 'target')

# Filter graph
G = fn.filter_for_k_core(G, k_cores=k_cores)
G = fn.filter_for_largest_components(G, num_comp=lccs)

# Communities and centralities
partition = community.best_partition(G)
dc = nx.degree_centrality(G)
bc = nx.betweenness_centrality(G, k=min(100,len(G)))
ec = nx.eigenvector_centrality(G, max_iter=1000)

# Set attributes (not necessisary)
nx.set_node_attributes(G, dc, 'cent_deg')
nx.set_node_attributes(G, bc, 'cent_bet')
nx.set_node_attributes(G, ec, 'cent_eig')
nx.set_node_attributes(G, partition, 'partition')

# Create dataframe for nodes and attributes
nodes = list(G.nodes())
nodes = pd.DataFrame(nodes)
nodes.columns = ['node']

# Map attributes to nodes dataframe
nodes['cent_deg'] = nodes['node'].map(dc)
nodes['cent_bet'] = nodes['node'].map(bc)
nodes['cent_eig'] = nodes['node'].map(ec)
nodes['partition'] = nodes['node'].map(partition)

# Using node ids rather than names (necessisary?)
nodes = nodes.reset_index()
nodes = nodes.rename(columns={'index': 'id'})

# Write edgelist to dataframe
edges = G.edges()
edges = pd.DataFrame(edges)
edges.columns = ['source', 'target']

# Get tweet text and assign to node
nodes['tweet_text'] = nodes['node'].apply(lambda x: fn.getText1(x, df))

# Get tweet id and assign to node
nodes['tweet_id'] = nodes['node'].apply(lambda x: fn.getTweetID(x, df))
nodes["tweet_id"] = nodes["tweet_id"].fillna(0)
nodes['tweet_id'] = nodes['tweet_id'].astype(int)
nodes['tweet_id'] = nodes['tweet_id'].astype(str)


# Rename and reorder to play nice with main.js (necessisary?)
nodes.columns = ['id', 'name', 'degree', 'betweenness', 'eigenvector', 'group', 'tweet_text', 'tweet_id']
nodes = nodes[['betweenness','degree','eigenvector','group','id','name','tweet_text', 'tweet_id']]

# Update edgelists to use ids (necessisary?)
edges['source'] = edges['source'].apply(lambda x: nodes.loc[nodes['name'] == x]['id'].values[0])
edges['target'] = edges['target'].apply(lambda x: nodes.loc[nodes['name'] == x]['id'].values[0])

# Convert nodes and edges to dictionaries
node_dict = nodes.to_dict(orient='records')
edge_dict = edges.to_dict(orient='records')
graph_dict = {'nodes': node_dict, 'links': edge_dict} # Because they are called links in main.js

# Write to json with datetime stamp
timestamp=datetime.datetime.now().strftime("%Y%m%d%H%M%S")
f=open(str(out_path) + 'preprocessed_%s.json'%timestamp,'w')     # MacOS path
json.dump(graph_dict,f)  # dump the tweets into a json file
f.close()

# Keep only two files
file_list = sorted(os.listdir(out_path))
if len(file_list) > 2:
	os.remove(str(out_path) + str(file_list[0]))
else:
	pass

import json
from flask import Flask, render_template, jsonify
import pandas as pd
import numpy as np
import networkx as nx
import community
import functions as fn
import yaml
#import listener
#import parameters
import codecs

with open('parameters.yaml') as file:
    parameters = yaml.full_load(file)
lccs = parameters['connected_components']
k_cores = parameters['k_cores']


application = Flask(__name__)

# 2. Declare data stores
class DataStore():
    foo = None

data = DataStore()

@application.route("/", methods=["GET", "POST"])
def index():
    #Read in data
    df = pd.read_json(codecs.open("raw_tweets.json", 'r', 'utf-8'))

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
    G = fn.filter_for_largest_components(G, num_comp=lccs)
    G = fn.filter_for_k_core(G, k_cores=k_cores)

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

    #In case want to test offline
    #with open("test.json", 'w', encoding='utf-8') as f:
    #    json.dump(graph_dict, f, ensure_ascii=False,indent=4)

    # Here comes the foo!
    data.foo = graph_dict
    return render_template("index.html")


@application.route("/get-data", methods=["GET", "POST"])
def returnProdData():
    f = data.foo
    return jsonify(f)

if __name__ == "__main__":
    application.run(debug=True)

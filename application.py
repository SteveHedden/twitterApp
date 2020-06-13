import json

from flask import Flask, render_template, jsonify
import pandas as pd
import numpy as np
import networkx as nx
import community
import functions as fn

application = Flask(__name__)

# 2. Declare data stores
class DataStore():
    foo = None

data = DataStore()

@application.route("/", methods=["GET", "POST"])
def index():
    #Read in data
    df = pd.read_json("1919-tweets.json")
    #df = pd.read_json("protest2020_tweets.json")


    # Create a source column in the df for the sn of the tweeter
    df['source'] = df["user"].apply(lambda x: fn.getScreenName(x))
    df['target'] = df['retweeted_status'].apply(lambda x: fn.getOrigScreenName(x))

    edges1 = pd.DataFrame()  # Create empty df

    # Break into retweets and non-reweets
    rts = df.loc[~pd.isnull(df["retweeted_status"])]
    nonrts = df.loc[pd.isnull(df["retweeted_status"])]

    # Create edges for rts
    edges1['source'] = rts["user"].apply(lambda x: fn.getScreenName(x))
    edges1['target'] = rts['retweeted_status'].apply(lambda x: fn.getOrigScreenName(x))

    # Create edges for mentions
    edges2 = pd.DataFrame()
    edges2['source'] = nonrts["user"].apply(lambda x: fn.getScreenName(x))
    edges2['target'] = nonrts['entities'].apply(lambda x: fn.getMentions(x))
    edges2 = edges2.dropna()

    edges = pd.concat([edges1, edges2])



    # Create graph using the data
    G = nx.from_pandas_edgelist(edges, 'source', 'target')

    # Partition graph based on 'best partition'
    partition = community.best_partition(G)

    
    # Turn partition into dataframe
    partition1 = pd.DataFrame([partition]).T
    partition1 = partition1.reset_index()
    partition1.columns = ['index', 'value']

    #Degree centrality
    dc = nx.degree_centrality(G)
    dc = pd.DataFrame([dc.keys(), dc.values()]).T
    dc.columns = ['names', 'values']  # call them whatever you like
    dc = dc.sort_values(by='values', ascending=False)
    dc1 = pd.merge(dc, partition1, how='left', left_on="names", right_on="index")

    #Betweenness centrality
    bc = nx.betweenness_centrality(G, k=100)
    bc = pd.DataFrame([bc.keys(), bc.values()]).T
    bc.columns = ['names', 'values']  # call them whatever you like
    bc = bc.sort_values(by='values', ascending=False)
    bc1 = pd.merge(bc, partition1, how='left', left_on="names", right_on="index")

    #Eigenvector centrality
    ec = nx.eigenvector_centrality(G, weight='freq', max_iter=1000)
    ec = pd.DataFrame([ec.keys(), ec.values()]).T
    ec.columns = ['names', 'values']
    ec = ec.sort_values(by='values', ascending=False)
    ec1 = pd.merge(ec, partition1, how='left', left_on="names", right_on="index")

    # Inputs are nodes data to filter on, name of file to save it as,
    # number of maximum nodes to take from each community, minimum centrality score
    # and minimum numbe of connections
    nodes, net = fn.createTopNodesforVisual(bc1, "test", 10000, 0.0001, 1, edges)

    # Make it exactly right for the d3 visual
    nodes = fn.buildNodesFromLinks(net, bc1)
    nodes = nodes.rename(columns={"cent": "betweenness", "value": "group", "id": "name"})
    nodes = nodes.reset_index()
    nodes = nodes.rename(columns={"index": "id"})
    net["source"] = net["source"].apply(lambda x: nodes.loc[nodes["name"] == x]["id"].values[0])
    net["target"] = net["target"].apply(lambda x: nodes.loc[nodes["name"] == x]["id"].values[0])

    nodes = pd.merge(nodes, ec1, how="left", left_on="name", right_on="names")
    nodes = nodes.drop(['names', 'index', 'value'], axis=1)
    nodes = nodes.rename(columns={"values": "eigenvector"})
    nodes = pd.merge(nodes, dc1, how="left", left_on="name", right_on="names")
    nodes = nodes.drop(['names', 'index', 'value'], axis=1)
    nodes = nodes.rename(columns={"values": "degree"})
    nodes = nodes[['betweenness', 'degree', 'eigenvector', 'group', 'id', 'name']]

    nodes["tweet_text"] = nodes["name"].apply(lambda x: fn.getText1(x, df))
    # Export data
    #fn.exportData(nodes, net, "test7")


    #nodes = pd.read_csv("nodes.csv")
    #net = pd.read_csv("net.csv")

    d1 = nodes.to_dict(orient='records')
    j1 = json.dumps(d1,indent=2)
    d2 = net.to_dict(orient='records')
    j2 = json.dumps(d2,indent=2)
    d1 = {"nodes": d1, "links": d2}
    data.foo = d1
    return render_template("index.html")


@application.route("/get-data", methods=["GET", "POST"])
def returnProdData():
    f = data.foo
    return jsonify(f)

if __name__ == "__main__":
    application.run(debug=True)

# Just for
import numpy as np
import networkx as nx
import pandas as pd
import json

# from preprocessor
import datetime
import yaml
import os
import community
import functions as fn


def getScreenName(x):
    try:
        temp = x["screen_name"]
        return temp;
    except:
        return;


def getOrigScreenName(x):
    try:
        temp = x["user"]["screen_name"]
        return temp;
    except:
        return;


def getMentions(x):
    try:
        temp = x["user_mentions"][0]["screen_name"]
        return temp;
    except:
        return np.NaN;


def filter_graph_for_viz(df, edges):
    top = df.groupby('value')['names'].filter(lambda x: len(x) > 0)
    df = df.loc[df['names'].isin(top)]
    degreeNetwork = build_graph_from_data(edges, df)
    nodes = buildNodesFromLinks(degreeNetwork, df)
    return (nodes, degreeNetwork)


def filter_for_largest_components(G, num_comp):
    G = nx.compose_all(sorted(nx.connected_component_subgraphs(G), key=len, reverse=True)[:num_comp])
    return G


def filter_for_k_core(G, k_cores):
    G.remove_edges_from(G.selfloop_edges())
    G_tmp = nx.k_core(G, k_cores)
    if len(G_tmp) <= 3:
        G_tmp = nx.k_core(G)
    return G_tmp


def getText1(x, df):
    length1 = len(df.loc[df["source"] == x]["text"])
    length2 = len(df.loc[df["target"] == x]["text"])
    if length1 > 0:
        text = df.loc[df["source"] == x]["text"].values[0]
        return text;
    elif length2 > 0:
        text = df.loc[df["target"] == x]["text"].values[0]
        return text;
    else:
        return;


def getTweetID(x, df):
    length1 = len(df.loc[df["source"] == x]["id_str"])
    length2 = len(df.loc[df["target"] == x]["id_str"])
    if length1 > 0:
        text = df.loc[df["source"] == x]["id_str"].values[0]
        return text;
    elif length2 > 0:
        text = df.loc[df["target"] == x]["id_str"].values[0]
        return text;
    else:
        return;


def concat_raw_files():
    # Load parameters
    with open('parameters.yaml') as file:
        parameters = yaml.full_load(file)
    project = parameters['project']
    raw_batch_size = parameters['raw_batch_size']

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
            f = open(raw_out_path + filename, 'r')
            ds = json.load(f)
            tweet_list.extend(ds)
            f.close()
        except:
            print('Error in: ' + str(filename))
    return tweet_list


# Process data into network for application.py
# file = str(out_path) + str(sorted(os.listdir(out_path))[0])

# df = pd.read_json(codecs.open(file, 'r', 'utf-8'))

def write_graph_dict(tweet_list):
    with open('parameters.yaml') as file:
        parameters = yaml.full_load(file)
    lccs = parameters['connected_components']
    k_cores = parameters['k_cores']
    project = parameters['project']
    out_path = 'data/' + str(project) + '/preprocessed/'

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
    #bc = nx.betweenness_centrality(G, k=min(100, len(G)))
    #ec = nx.eigenvector_centrality(G, max_iter=1000)
    #ec = nx.eigenvector_centrality(G)

    # Set attributes (not necessisary)
    nx.set_node_attributes(G, dc, 'cent_deg')
    #nx.set_node_attributes(G, bc, 'cent_bet')
    #nx.set_node_attributes(G, ec, 'cent_eig')
    nx.set_node_attributes(G, partition, 'partition')

    # Create dataframe for nodes and attributes
    nodes = list(G.nodes())
    nodes = pd.DataFrame(nodes)
    nodes.columns = ['node']

    # Map attributes to nodes dataframe
    nodes['cent_deg'] = nodes['node'].map(dc)
    #nodes['cent_bet'] = nodes['node'].map(bc)
    #nodes['cent_eig'] = nodes['node'].map(ec)
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
    # nodes['tweet_id'] = nodes['tweet_id'].astype(int)
    # nodes['tweet_id'] = nodes['tweet_id'].astype(str)

    # Rename and reorder to play nice with main.js (necessisary?)
    #nodes.columns = ['id', 'name', 'degree', 'betweenness', 'eigenvector', 'group', 'tweet_text', 'tweet_id']
    #nodes = nodes[['betweenness', 'degree', 'eigenvector', 'group', 'id', 'name', 'tweet_text', 'tweet_id']]
    nodes.columns = ['id', 'name', 'degree', 'group', 'tweet_text', 'tweet_id']
    nodes = nodes[['degree', 'group', 'id', 'name', 'tweet_text', 'tweet_id']]

    # Update edgelists to use ids (necessisary?)
    edges['source'] = edges['source'].apply(lambda x: nodes.loc[nodes['name'] == x]['id'].values[0])
    edges['target'] = edges['target'].apply(lambda x: nodes.loc[nodes['name'] == x]['id'].values[0])

    # Convert nodes and edges to dictionaries
    node_dict = nodes.to_dict(orient='records')
    edge_dict = edges.to_dict(orient='records')
    graph_dict = {'nodes': node_dict, 'links': edge_dict}  # Because they are called links in main.js

    # Write to json with datetime stamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    f = open(str(out_path) + 'preprocessed_%s.json' % timestamp, 'w')  # MacOS path
    json.dump(graph_dict, f)  # dump the tweets into a json file
    f.close()

    # Keep only two files
    file_list = sorted(os.listdir(out_path))
    if len(file_list) > 2:
        os.remove(str(out_path) + str(file_list[0]))
    else:
        pass
    return


'''def build_graph_from_data(G, df):
    df = G.loc[(G['source'].isin(df['names'])) & (G['target'].isin(df['names']))]
    G = nx.from_pandas_edgelist(df, 'source', 'target')
    G = filter_for_largest_components(G, lccs) # second argument is number of connected componenets
    G = filter_for_k_core(G, k_cores)
    edgelist = nx.to_pandas_edgelist(G)
    df_edgelist = pd.DataFrame(edgelist[['source', 'target']])
    return df_edgelist

def createTopNodesforVisual(df, nameOfFile, head, threshold, minDegree, edges):
    # Only use groups with more than X members
    top = df.groupby('value')['names'].filter(lambda x: len(x) > 0)
    df = df.loc[df['names'].isin(top)]
    degreeNetwork = filterByPartitionAndCentrality(df, head, threshold)
    degreeNetwork = degreeNetwork.sort_values(by="values", ascending=False).head(head)
    singles = degreeNetwork.groupby('value')['names'].filter(lambda x: len(x) < minDegree)
    degreeNetwork = degreeNetwork.loc[~degreeNetwork['names'].isin(singles)]
    degreeNetwork = buildNetworkFromData(edges, degreeNetwork, minDegree)
    nodes = buildNodesFromLinks(degreeNetwork, df)
    # exportData(nodes,degreeNetwork,nameOfFile)
    return (nodes, degreeNetwork)

def filterByPartitionAndCentrality(df, head, centralityThreshold):
    df = df.groupby('value').head(head)
    # df = df.head(head)
    df = df.loc[df['values'] > centralityThreshold]
    return df

def buildNetworkFromData(network, df, minDegree):
    df = network.loc[(network['source'].isin(df['names'])) & (network['target'].isin(df['names']))]
    G_clean = nx.from_pandas_edgelist(df, 'source', 'target')
    remove = [node for node, degree in dict(G_clean.degree()).items() if degree < minDegree]
    G_clean.remove_nodes_from(remove)
    G_clean = nx.to_pandas_edgelist(G_clean)
    # G_clean = pd.merge(G_clean,names,how='left',left_on='source',right_on='nconst')
    # G_clean = pd.merge(G_clean,names,how='left',left_on='target',right_on='nconst')
    G_clean = pd.DataFrame(G_clean[['source', 'target']])
    # G_clean.columns = ['source','target']
    # G_clean = G_clean.dropna()
    # G_clean = G_clean.drop_duplicates()
    return G_clean

def buildNodesFromLinks(df, centralityData):
    nodes1 = pd.DataFrame(df['source'])
    nodes2 = pd.DataFrame(df['target'])
    nodes2.columns = ['source']
    nodes = pd.concat([nodes1, nodes2])
    nodes = nodes.drop_duplicates()
    nodes2 = pd.merge(nodes, centralityData, how='left', left_on='source', right_on='names')
    nodes2 = nodes2.dropna()
    nodes2 = pd.DataFrame(nodes2[['names', 'values', 'value']])
    nodes2.columns = ['id', 'cent', 'value']
    return nodes2

def exportData(nodes, network, fileName):
    d1 = nodes.to_dict(orient='records')
    j1 = json.dumps(d1)
    d2 = network.to_dict(orient='records')
    j2 = json.dumps(d2)
    d1 = {"nodes": d1, "links": d2}
    with open(fileName + ".json", 'w', encoding='utf-8') as f:
        json.dump(d1, f, ensure_ascii=False, indent=4)'''


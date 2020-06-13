import numpy as np
import networkx as nx
import pandas as pd
import json
import yaml

# Import parameters 
with open('parameters.yaml') as file:
    parameters = yaml.full_load(file)
lccs = parameters['connected_components']
k_cores = parameters['k_cores']

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
    degreeNetwork = build_network_from_data(edges, df)
    nodes = buildNodesFromLinks(degreeNetwork, df)
    return (nodes, degreeNetwork)

def build_network_from_data(network, df):
    df = network.loc[(network['source'].isin(df['names'])) & (network['target'].isin(df['names']))]
    network = nx.from_pandas_edgelist(df, 'source', 'target')
    network = filter_for_largest_components(network, lccs) # second argument is number of connected componenets
    G_clean = filter_for_k_core(network, k_cores)
    edgelist = nx.to_pandas_edgelist(network)
    df_edgelist = pd.DataFrame(edgelist[['source', 'target']])
    return df_edgelist

def filter_for_largest_components(network, num_comp):
    network = nx.compose_all(sorted(nx.connected_component_subgraphs(network), key = len, reverse=True)[:num_comp])
    return network

def filter_for_k_core(network, k_cores):
    network.remove_edges_from(network.selfloop_edges())
    #network = nx.k_core(network, k_cores)
    network = nx.connected_component_subgraphs(nx.k_core(network, k=k_cores))

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
        json.dump(d1, f, ensure_ascii=False, indent=4)

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
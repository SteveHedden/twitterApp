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

def getScreenName2(x):
    if pd.isnull(x['retweeted_status']):
        try:
            temp = x['user']['screen_name']
        except:
            return
    else:
        temp = x['retweeted_status']['user']['screen_name']
    return temp


def getMentions(x):
    try:
        temp = x["user_mentions"][0]["screen_name"]
        return temp;
    except:
        return np.NaN;

def rtCount(x):
    if pd.isnull(x):
        return 0
    else:
        count = x['retweet_count']
        return count

def getMaxRTs(x, df):
    tweets = df.loc[df['original_screen_name'] == x]
    tweets = tweets.reset_index()
    max = str(tweets['rt_count'].max())
    return max


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

"""

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

def getText1(x, df):
    tweets = df.loc[df['original_screen_name'] == x]
    tweets = tweets.reset_index()
    max = 0
    try:
        if ~pd.isnull(tweets['retweeted_status'][0]):
            for x in tweets['retweeted_status']:
                temp = x['retweet_count']
                if temp > 0:
                    max = temp
            for x in tweets['retweeted_status']:
                if x['retweet_count'] == max:
                    rts = x['text']
            return rts
        else:
            #text = tweets['text']
            return "null"
    except:
        #text = tweets['text']
        return "null2"



def getTweetID(x, df):
    id = str(df.loc[df['original_screen_name'] == x]['id_str'].values[0])
    return id
"""
def getTweetID(x, df):
    tweets = df.loc[df['original_screen_name'] == x]
    tweets = tweets.reset_index()
    # First deal with people who tweeted but had no RTs (losers)
    if tweets['rt_count'].sum() == 0:
        # I think a lot of these may be bots. They have tweeted multiple times in a short
        # period of time with no RTs. So they are eithe bots or suck at twitter.
        if len(tweets) > 1:
            # print(tweets['original_screen_name'])
            return str(tweets.iloc[0]['text'])
        if len(tweets) == 1:
            return str(tweets['text'])
    else:
        # This means that this person was retweeted during this interval. We want the
        # max value of RTs looking at all their instances.

        # First get rid of all original tweets since that doesn't give us any information
        tweets = tweets.loc[tweets['rt_count'] > 0]
        max = tweets['rt_count'].max()
        id = str(tweets.loc[tweets['rt_count'] == max]['id_str'].values[0])
        return id

def getText1(x, df):
    tweets = df.loc[df['original_screen_name'] == x]
    tweets = tweets.reset_index()
    # First deal with people who tweeted but had no RTs (losers)
    if tweets['rt_count'].sum() == 0:
        # I think a lot of these may be bots. They have tweeted multiple times in a short
        # period of time with no RTs. So they are eithe bots or suck at twitter.
        if len(tweets) > 1:
            # print(tweets['original_screen_name'])
            return str(tweets.iloc[0]['text'])
        if len(tweets) == 1:
            return str(tweets['text'])
    else:
        # This means that this person was retweeted during this interval. We want the
        # max value of RTs looking at all their instances.

        # First get rid of all original tweets since that doesn't give us any information
        tweets = tweets.loc[tweets['rt_count'] > 0]
        max = tweets['rt_count'].max()
        text = str(tweets.loc[tweets['rt_count'] == max]['text'].values[0])
        return text

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

def getNodes(G, dc, partition, df):
    # Set attributes (not necessisary)
    nx.set_node_attributes(G, dc, 'cent_deg')
    nx.set_node_attributes(G, partition, 'partition')

    # Create dataframe for nodes and attributes
    nodes = list(G.nodes())
    nodes = pd.DataFrame(nodes)
    nodes.columns = ['node']

    # Map attributes to nodes dataframe
    nodes['cent_deg'] = nodes['node'].map(dc)
    nodes['partition'] = nodes['node'].map(partition)

    # Using node ids rather than names (necessary?)
    nodes = nodes.reset_index()
    nodes = nodes.rename(columns={'index': 'id'})

    # Get tweet text and assign to node
    nodes['tweet_text'] = nodes['node'].apply(lambda x: fn.getText1(x, df))

    # Get tweet id and assign to node
    nodes['tweet_id'] = nodes['node'].apply(lambda x: fn.getTweetID(x, df))
    nodes["tweet_id"] = nodes["tweet_id"].fillna(0)

    # Rename and reorder to play nice with main.js (necessisary?)
    nodes.columns = ['id', 'name', 'degree', 'group', 'tweet_text', 'tweet_id']
    nodes = nodes[['degree', 'group', 'id', 'name', 'tweet_text', 'tweet_id']]

    return nodes


def cleanRawData(df):
    #Create a new column for total RT count. Needed to get TOP tweet text and ID
    #from users with multiple tweets and/or retweets
    df['rt_count'] = df['retweeted_status'].apply(lambda x: rtCount(x))

    df['original_screen_name'] = df.apply(lambda x: fn.getScreenName2(x), axis=1)
    return df

def write_graph_dict(tweet_list):
    with open('parameters.yaml') as file:
        parameters = yaml.full_load(file)
    lccs = parameters['connected_components']
    k_cores = parameters['k_cores']
    project = parameters['project']
    out_path = 'data/' + str(project) + '/preprocessed/'

    df = pd.DataFrame(tweet_list)

    # Create a source column in the df for the sn of the tweeter
    #df['source'] = df['user'].apply(lambda x: fn.getScreenName(x))
    #df['target'] = df['retweeted_status'].apply(lambda x: fn.getOrigScreenName(x))

    df = cleanRawData(df)
    #df['source'] = df['original_screen_name']
    #df['target'] = df['source']

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

    nodes = fn.getNodes(G, dc, partition, df)

    # Get top groups
    topGroups = nodes.sort_values(['degree'], ascending=False)['group'].head(100).values[:]
    topGroups = nodes['group'].unique().tolist()

    # Write to master df
    fn.buildCommunityData(nodes, topGroups, df)

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

def buildCommunityData(nodes, topGroups, df):
    with open('parameters.yaml') as file:
        parameters = yaml.full_load(file)
    communities_path = 'data/' + str(parameters['project']) + '/communities/'
    if not os.path.exists(communities_path):
        os.makedirs(communities_path)
    project = parameters['project']
    out_path = 'data/' + str(project) + '/communities/'
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    df1 = pd.DataFrame()
    for group in topGroups:
        # pre-process text and store the same
        temp = nodes.loc[nodes["group"] == group]

        df2 = pd.DataFrame(
            columns=['group', 'influencers', 'tweet_text', 'tweet_id'])

        # influencers
        temp = temp.sort_values(['degree'], ascending=False)
        influencers = list(temp['name'][:5])
        df2['influencers'] = influencers

        df2['group'] = group
        timestamp = pd.datetime.now().replace(microsecond=0)
        #df2["date"] = str(timestamp)
        df2.insert(0, 'date', timestamp)
        df2['date'] = df2['date'].astype(str)

        df2['tweet_text'] = df2['influencers'].apply(lambda x: fn.getText1(x, df))
        df2['tweet_id'] = df2['influencers'].apply(lambda x: fn.getTweetID(x, df))
        df2['number_of_retweets'] = df2['influencers'].apply(lambda x: fn.getMaxRTs(x, df))

        df1 = df1.append(df2)
        # Write to csv with datetime stamp
    df1 = df1.to_dict(orient="records")
    # df1.to_csv(str(out_path) + "communities" + timestamp + ".csv")  # MacOS path
    f = open(str(out_path) + 'communities_%s.json' % timestamp, 'w')  # MacOS path
    json.dump(df1, f)  # dump the tweets into a json file
    f.close()
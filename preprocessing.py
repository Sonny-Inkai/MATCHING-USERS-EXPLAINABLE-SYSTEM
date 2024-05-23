import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from confluent_kafka import Consumer
import scipy
import matplotlib as plt
import networkx as nx
import itertools
import collections
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
import numpy as np
from pinecone import Pinecone, ServerlessSpec
from config import get_config

def convert_categorical(df_X, _X):
    values = np.array(df_X[_X])
    # integer encode
    label_encoder = LabelEncoder()
    integer_encoded = label_encoder.fit_transform(values)
    # binary encode
    onehot_encoder = OneHotEncoder(sparse=False)
    integer_encoded = integer_encoded.reshape(len(integer_encoded), 1)
    onehot_encoded = onehot_encoder.fit_transform(integer_encoded)
    df_X = df_X.drop(_X, 1)
    for j in range(integer_encoded.max() + 1):
        df_X.insert(loc=j + 1, column=str(_X) + str(j + 1), value=onehot_encoded[:, j])
    return df_X

def graph_based_process(df_users, df, alpha_coef):

    df_users = convert_categorical(df_users, 'job')
    df_users = convert_categorical(df_users, 'gender')
    df_users['bin'] = pd.cut(df_users['age'], [0, 10, 20, 30, 40, 50, 100], labels=['1', '2', '3', '4', '5', '6'])
    df_users['age'] = df_users['bin']

    df_users = df_users.drop('bin', 1)
    df_users = convert_categorical(df_users, 'age')
    df_users = df_users.drop('zip', 1)

    pairs = []
    grouped = df.groupby(['MID', 'rate'])
    for key, group in grouped:
        pairs.extend(list(itertools.combinations(group['UID'], 2)))
    counter = collections.Counter(pairs)
    alpha = alpha_coef * 1682  # param*i_no
    edge_list = map(list, collections.Counter(el for el in counter.elements() if counter[el] >= alpha).keys())
    G = nx.Graph()

    for el in edge_list:
        G.add_edge(el[0], el[1], weight=1)
        G.add_edge(el[0], el[0], weight=1)
        G.add_edge(el[1], el[1], weight=1)

    pr = nx.pagerank(G.to_directed())
    df_users['PR'] = df_users['UID'].map(pr)
    df_users['PR'] /= float(df_users['PR'].max())
    dc = nx.degree_centrality(G)
    df_users['CD'] = df_users['UID'].map(dc)
    df_users['CD'] /= float(df_users['CD'].max())
    cc = nx.closeness_centrality(G)
    df_users['CC'] = df_users['UID'].map(cc)
    df_users['CC'] /= float(df_users['CC'].max())
    bc = nx.betweenness_centrality(G)
    df_users['CB'] = df_users['UID'].map(bc)
    df_users['CB'] /= float(df_users['CB'].max())
    lc = nx.load_centrality(G)
    df_users['LC'] = df_users['UID'].map(lc)
    df_users['LC'] /= float(df_users['LC'].max())
    nd = nx.average_neighbor_degree(G, weight='weight')
    df_users['AND'] = df_users['UID'].map(nd)
    df_users['AND'] /= float(df_users['AND'].max())
    X_train = df_users[df_users.columns[1:]]
    X_train.fillna(0, inplace=True)

    return X_train

# Store User's vectors into PineCone Vector database
def update_vectordb(users, index_name, pinecone_cf):
    pc = Pinecone(api_key=pinecone_cf)

    retriver = pc.Index(index_name)
    vectors = [{"id": str(userid + 1), "values": uservector.values} for userid, uservector in users.iterrows()]
    retriver.upsert(vectors=vectors)
    print("Update Vectordb Successfully!")

if __name__ == "__main__":
    config = get_config()
    mongo_uri = config['mongo_uri']
    client = MongoClient(mongo_uri, server_api=ServerApi('1'))
    database_name = "persona"
    collection_table1 = "rating"
    collection_table2 = "user_infor"
    db = client[database_name]
    user_table = db[collection_table2]
    rating_table = db[collection_table1]

    df_users = pd.DataFrame(list(user_table.find())).drop(columns=['_id'], axis=0)
    df_users.dropna(inplace=True)
    df_ratings = pd.DataFrame(list(rating_table.find())).drop(columns=['_id'], axis=0)
    df_ratings.dropna(inplace=True)

    # Create retriver index
    alpha_coefs = [0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.035, 0.04, 0.045] # reference values
    index_name = "user-vectors"
    user_vectors = graph_based_process(df_users=df_users, df=df_ratings, alpha_coef=0.01)
    update_vectordb(users=user_vectors, index_name=index_name, pinecone_cf=config['pinecone_api'])








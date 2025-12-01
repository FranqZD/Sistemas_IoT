import pydgraph
import time
from cassandra.cluster import Cluster
from pymongo import MongoClient


from Cassandra import cModel
from Dgraph import dModel
from Mongo import mModel
 
#Cassandra data
CLUSTER_IPS = ['127.0.0.1']
KEYSPACE = 'iotsystem'
REPLICATION_FACTOR = 1


#MongoDB data
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "IoTSystemDB"


def init_mongo():
           
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    mModel.create_mongo_indexes(db)
    print("MongoDB conectado a la base de datos:", MONGO_DB_NAME)
    return db


def init_dgraph():
    client_stub = pydgraph.DgraphClientStub('localhost:9080')
    client = pydgraph.DgraphClient(client_stub)
    return client

def init_cassandra():
    cluster = Cluster(CLUSTER_IPS)
    session = cluster.connect()
    cModel.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)
    cModel.create_schema(session)
    return session
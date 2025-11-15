import pydgraph
from cassandra.cluster import Cluster

from Cassandra.cModel import model as cModel
from Dgraph.dModel import model as dModel
from Mongo.mModel import model as mModel
 
#Cassandra data
CLUSTER_IPS = ['127.0.0.1']
KEYSPACE = 'IotSystem'
REPLICATION_FACTOR = 1

def init_mongo():
    pass

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
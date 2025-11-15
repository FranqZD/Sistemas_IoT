import os

from cassandra.cluster import Cluster

from Cassandra.cModel import model as cModel
from Dgraph.dModel import model as dModel
from Mongo.mModel import model as mModel
 
#Cassandra

# Read env vars related to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', '127.0.0.1')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'logistics')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

def init_cassandra():
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    cModel.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    cModel.create_schema(session)

    return session

def main():
    init_cassandra()
    print('Connected to Cluster :)')


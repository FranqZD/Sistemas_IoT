import pydgraph
import csv
def create_Schema(client):
    schema = """
name: string @index(exact) .
description: string .
type: string @index(hash) .
status: string @index(hash) .
location: geo @index(geo) .
technical_contact: string .

Contains_Cluster: [uid] @reverse .
Contains_Place: [uid] @reverse .
Contains_Device: [uid] @reverse .
Has_Brand: [uid] @reverse .
Connected_With: [uid] @reverse .

type Zone {
  name
  location
  description
  Contains_Cluster
}

type Cluster {
  name
  type
  status
  location
  Contains_Place
}

type Place {
  name
  type
  location
  Contains_Device
}

type IoTDevice {
  name
  type
  status
  Has_Brand
  Connected_With
}

type Brand {
  name
  description
  technical_contact
}
    """
    client.alter(pydgraph.Operation(schema=schema))
    print("Schema created.")
    txn = client.txn()
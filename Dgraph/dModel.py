import pydgraph
import json
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
    txn = client.txn()
 
 
 # --- SECCIÓN: PLACES (Instalaciones) ---

def get_places_by_cluster(client, cluster_name):
    query = f"""{{
      res(func: eq(name, "{cluster_name}")) {{
        name
        type
        Contains_Place {{
          name
          type
          location
        }}
      }}
    }}"""
    
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        print(f"\n--- Instalaciones en {cluster_name} ---")
        print(json.dumps(data, indent=2))
    finally:
        txn.discard()

def get_places_related_to_zone(client, zone_name):
    query = f"""{{
      res(func: eq(name, "{zone_name}")) {{
        name
        Contains_Cluster {{
          name
          Contains_Place {{
            name
            type
          }}
        }}
      }}
    }}"""

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        print(f"\n--- Instalaciones relacionadas a {zone_name} ---")
        print(json.dumps(data, indent=2))
    finally:
        txn.discard()

def get_places_by_type_in_cluster(client, cluster_name, place_type):
    query = f"""{{
      res(func: eq(name, "{cluster_name}")) {{
        name
        Contains_Place @filter(eq(type, "{place_type}")) {{
          name
          type
        }}
      }}
    }}"""

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        print(f"\n--- Instalaciones tipo '{place_type}' en {cluster_name} ---")
        print(json.dumps(data, indent=2))
    finally:
        txn.discard()

# --- SECCIÓN: CLUSTERS ---

def get_clusters_by_status_in_zone(client, zone_name, status):
    query = f"""{{
      res(func: eq(name, "{zone_name}")) {{
        name
        Contains_Cluster @filter(eq(status, "{status}")) {{
          name
          status
          type
        }}
      }}
    }}"""

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        print(f"\n--- Clusters '{status}' en {zone_name} ---")
        print(json.dumps(data, indent=2))
    finally:
        txn.discard()

def get_all_clusters_in_zone(client, zone_name):
    query = f"""{{
      res(func: eq(name, "{zone_name}")) {{
        name
        Contains_Cluster {{
          name
          type
          status
        }}
      }}
    }}"""

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        print(f"\n--- Todos los clusters en {zone_name} ---")
        print(json.dumps(data, indent=2))
    finally:
        txn.discard()

# --- SECCIÓN: ZONES ---

def get_zones_with_brand_devices(client, brand_name):
   pass


# --- SECCIÓN: DEVICES (Dispositivos) ---

def get_place_of_device(client, device_name):
    query = f"""{{
      res(func: eq(name, "{device_name}")) {{
        name
        uid
        ~Contains_Device {{
          name
          type
          location
        }}
      }}
    }}"""

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        print(f"\n--- ¿A qué instalación pertenece {device_name}? ---")
        print(json.dumps(data, indent=2))
    finally:
        txn.discard()

def get_inactive_devices_in_cluster(client, cluster_name):
    query = f"""{{
      res(func: eq(name, "{cluster_name}")) {{
        name
        Contains_Place {{
          name
          Contains_Device @filter(eq(status, "OFF")) {{
            name
            status
            type
          }}
        }}
      }}
    }}"""

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        print(f"\n--- Dispositivos APAGADOS en {cluster_name} ---")
        print(json.dumps(data, indent=2))
    finally:
        txn.discard()

def get_device_connections(client, device_name):
    query = f"""{{
      res(func: eq(name, "{device_name}")) {{
        name
        Connected_With {{
          name
          type
        }}
      }}
    }}"""

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)
        print(f"\n--- Conexiones de {device_name} ---")
        print(json.dumps(data, indent=2))
    finally:
        txn.discard()

def count_devices_in_cluster(client, cluster_name):
    query = f"""{{
      res(func: eq(name, "{cluster_name}")) {{
        name
        Contains_Place {{
          name
          num_devices: count(Contains_Device)
        }}
      }}
    }}"""

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        data = json.loads(res.json)

        clusters = data.get('res', [])

        places = clusters[0].get('Contains_Place', [])
        
        total_acumulado = 0
        
        print(f"\n---({cluster_name})---")
        
        for place in places:
            nombre_lugar = place.get('name')
            conteo_individual = place.get('num_devices', 0)
            
            print(f"{nombre_lugar}: {conteo_individual}")
            
            total_acumulado += conteo_individual

        print(f"TOTAL FINAL SUMADO: {total_acumulado}")

    finally:
        txn.discard()

def get_devices_by_brand_in_zone(client, zone_name, brand_name):
  pass
import csv
import json
import connect as connect

#Parseador para location
def parse_location(loc_str):
    if not loc_str or loc_str == "":
        return None
    try:
        return json.loads(loc_str)
    except json.JSONDecodeError:
        print(f"Error parseando location: {loc_str}")
        return None

# --- 1. Load BRANDS ---
def load_brands(file_path, client):
    txn = client.txn()
    uid_map = {} 
    temp_ref = {}

    try:
        nodes = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                clean_key = row['name']
                temp_ref[clean_key] = row['name']
                
                nodes.append({
                    'uid': '_:' + clean_key,
                    'dgraph.type': 'Brand',
                    'name': row['name'],
                    'description': row['description'],
                    'technical_contact': row['technical_contact']
                })
        
        if nodes:
            res = txn.mutate(set_obj=nodes)
            txn.commit()
            
            for key, real_uid in res.uids.items():
                original_name = temp_ref.get(key)
                if original_name:
                    uid_map[original_name] = real_uid
            
            print(f"Brands cargadas y mapeadas: {len(uid_map)}")
    finally:
        txn.discard()
    
    return uid_map

# --- 2. Load ZONES ---
def load_zones(file_path, client):
    txn = client.txn()
    uid_map = {}
    temp_ref = {}

    try:
        nodes = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                clean_key = row['name']
                temp_ref[clean_key] = row['name']
                
                nodes.append({
                    'uid': '_:' + clean_key,
                    'dgraph.type': 'Zone',
                    'name': row['name'],
                    'description': row['description'],
                    'location': parse_location(row['location'])
                })
        
        if nodes:
            res = txn.mutate(set_obj=nodes)
            txn.commit()
            for key, real_uid in res.uids.items():
                original_name = temp_ref.get(key)
                if original_name:
                    uid_map[original_name] = real_uid
            print(f"Zones cargadas y mapeadas: {len(uid_map)}")
    finally:
        txn.discard()
    return uid_map

# --- 3. Load CLUSTERS ---
def load_clusters(file_path, client):
    txn = client.txn()
    uid_map = {}
    temp_ref = {}

    try:
        nodes = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                clean_key = row['name']
                temp_ref[clean_key] = row['name']

                nodes.append({
                    'uid': '_:' + clean_key,
                    'dgraph.type': 'Cluster',
                    'name': row['name'],
                    'type': row['type'],
                    'status': row['status'],
                    'location': parse_location(row['location'])
                })
        
        if nodes:
            res = txn.mutate(set_obj=nodes)
            txn.commit()
            for key, real_uid in res.uids.items():
                original_name = temp_ref.get(key)
                if original_name:
                    uid_map[original_name] = real_uid
            print(f"Clusters cargados y mapeados: {len(uid_map)}")
    finally:
        txn.discard()
    return uid_map

# --- 4. Load PLACES ---
def load_places(file_path, client):
    txn = client.txn()
    uid_map = {}
    temp_ref = {}

    try:
        nodes = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                clean_key = row['name']
                temp_ref[clean_key] = row['name']

                nodes.append({
                    'uid': '_:' + clean_key,
                    'dgraph.type': 'Place',
                    'name': row['name'],
                    'type': row['type'],
                    'location': parse_location(row['location'])
                })
        
        if nodes:
            res = txn.mutate(set_obj=nodes)
            txn.commit()
            for key, real_uid in res.uids.items():
                original_name = temp_ref.get(key)
                if original_name:
                    uid_map[original_name] = real_uid
            print(f"Places cargados y mapeados: {len(uid_map)}")
    finally:
        txn.discard()
    return uid_map

# --- 5. Load DEVICES ---
def load_devices(file_path, client):
    txn = client.txn()
    uid_map = {}
    temp_ref = {}

    try:
        nodes = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                device_name = row['device_alias']
                clean_key = device_name
                temp_ref[clean_key] = device_name

                nodes.append({
                    'uid': '_:' + clean_key,
                    'dgraph.type': 'IoTDevice',
                    'name': device_name,
                    'type': row['type'],
                    'status': row['status']
                })
        
        if nodes:
            res = txn.mutate(set_obj=nodes)
            txn.commit()
            for key, real_uid in res.uids.items():
                original_name = temp_ref.get(key)
                if original_name:
                    uid_map[original_name] = real_uid
            print(f"Devices cargados y mapeados: {len(uid_map)}")
    finally:
        txn.discard()
    return uid_map

# 1. Connected_With (Device -> Device)
def create_connected_with_edges(file_path, client, device_uids):
    txn = client.txn()
    try:
        with open(file_path, 'r', encoding='utf_8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # device = Origen, cdevice = Destino
                src_name = row['device']
                tgt_name = row['cdevice']
                
                # Verificamos que existan en el mapa de UIDs
                if src_name in device_uids and tgt_name in device_uids:
                    mutation = {
                        'uid': device_uids[src_name],
                        'Connected_With': {
                            'uid': device_uids[tgt_name]
                        }
                    }
                    print(f"Generating relationship {src_name} -Connected_With-> {tgt_name}")
                    txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()

# 2. Contains_Cluster (Zone -> Cluster)
def create_zone_cluster_edges(file_path, client, zone_uids, cluster_uids):
    txn = client.txn()
    try:
        with open(file_path, 'r', encoding='utf_8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                z_name = row['zone']
                c_name = row['cluster']
                
                if z_name in zone_uids and c_name in cluster_uids:
                    mutation = {
                        'uid': zone_uids[z_name],
                        'Contains_Cluster': {
                            'uid': cluster_uids[c_name]
                        }
                    }
                    print(f"Generating relationship {z_name} -Contains_Cluster-> {c_name}")
                    txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()

# 3. Contains_Device (Place -> Device)
def create_place_device_edges(file_path, client, place_uids, device_uids):
    txn = client.txn()
    try:
        with open(file_path, 'r', encoding='utf_8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                p_name = row['place']
                d_name = row['device']
                
                if p_name in place_uids and d_name in device_uids:
                    mutation = {
                        'uid': place_uids[p_name],
                        'Contains_Device': {
                            'uid': device_uids[d_name]
                        }
                    }
                    print(f"Generating relationship {p_name} -Contains_Device-> {d_name}")
                    txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()

# 4. Contains_Place (Cluster -> Place)
def create_cluster_place_edges(file_path, client, cluster_uids, place_uids):
    txn = client.txn()
    try:
        with open(file_path, 'r', encoding='utf_8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                c_name = row['cluster']
                p_name = row['place']
                
                if c_name in cluster_uids and p_name in place_uids:
                    mutation = {
                        'uid': cluster_uids[c_name],
                        'Contains_Place': {
                            'uid': place_uids[p_name]
                        }
                    }
                    print(f"Generating relationship {c_name} -Contains_Place-> {p_name}")
                    txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()

# 5. Has_Brand (Device -> Brand)
def create_device_brand_edges(file_path, client, device_uids, brand_uids):
    txn = client.txn()
    try:
        with open(file_path, 'r', encoding='utf_8-sig') as file:
            reader = csv.DictReader(file)
            for row in reader:
                d_name = row['device']
                b_name = row['brand']
                
                if d_name in device_uids and b_name in brand_uids:
                    mutation = {
                        'uid': device_uids[d_name],
                        'Has_Brand': {
                            'uid': brand_uids[b_name]
                        }
                    }
                    print(f"Generating relationship {d_name} -Has_Brand-> {b_name}")
                    txn.mutate(set_obj=mutation)
        txn.commit()
    finally:
        txn.discard()
        
def load_data(client):
    print("--- 1. CARGANDO NODOS Y OBTENIENDO UIDs ---")
    
    # Cada función inserta y retorna el diccionario { 'Nombre': 'UID' }
    map_brands = load_brands('Dgraph/data/Brand.csv', client)
    map_zones = load_zones('Dgraph/data/Zone.csv', client)
    map_clusters = load_clusters('Dgraph/data/Cluster.csv', client)
    map_places = load_places('Dgraph/data/Place.csv', client)
    map_devices = load_devices('devices.csv', client)

    print("\n--- 2. CREANDO RELACIONES USANDO MAPAS ---")
    
    create_zone_cluster_edges(
        'Dgraph/data/Contains_cluster.csv', client, 
        map_zones, map_clusters
    )
    
    create_cluster_place_edges(
        'Dgraph/data/Contains_place.csv', client, 
        map_clusters, map_places
    )

    create_place_device_edges(
        'Dgraph/data/Contains_device.csv', client, 
        map_places, map_devices
    )

    create_device_brand_edges(
        'Dgraph/data/Has_brand.csv', client, 
        map_devices, map_brands
    )

    create_connected_with_edges(
        'Dgraph/data/Connected_with.csv', client, 
        map_devices
    )

    print("\n--- ¡RELACIONES COMPLETAS!!! ---")
    
if __name__ == '__main__':
    
    # Ejecutar
    load_data(connect.init_dgraph())
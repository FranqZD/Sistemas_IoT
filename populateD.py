import csv
import connect as connect

# -------- LOAD NODES FIXED -------- #

def load_brands(file_path, client):
    txn = client.txn()
    uid_map = {}
    try:
        nodes = []
        with open(file_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                brand = row["name"]
                nodes.append({
                    "uid": f"_:brand_{brand}",
                    "dgraph.type": "Brand",
                    "name": brand
                })
        if nodes:
            res = txn.mutate(set_obj=nodes)
            txn.commit()
            for key, u in res.uids.items():
                cleaned = key.replace("brand_", "")
                uid_map[cleaned] = u
    finally:
        txn.discard()
    return uid_map


def load_zones(file_path, client):
    txn = client.txn()
    uid_map = {}
    try:
        nodes = []
        with open(file_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                zone = row["name"]
                nodes.append({
                    "uid": f"_:zone_{zone}",
                    "dgraph.type": "Zone",
                    "name": zone
                })
        if nodes:
            res = txn.mutate(set_obj=nodes)
            txn.commit()
            for key, u in res.uids.items():
                cleaned = key.replace("zone_", "")
                uid_map[cleaned] = u
    finally:
        txn.discard()
    return uid_map


def load_clusters(file_path, client):
    txn = client.txn()
    uid_map = {}
    try:
        nodes = []
        with open(file_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                c = row["name"]
                nodes.append({
                    "uid": f"_:cluster_{c}",
                    "dgraph.type": "Cluster",
                    "name": c
                })
        if nodes:
            res = txn.mutate(set_obj=nodes)
            txn.commit()
            for key, u in res.uids.items():
                cleaned = key.replace("cluster_", "")
                uid_map[cleaned] = u
    finally:
        txn.discard()
    return uid_map


def load_places(file_path, client):
    txn = client.txn()
    uid_map = {}
    try:
        nodes = []
        with open(file_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                place = row["name"]
                ptype = row["type"]
                nodes.append({
                    "uid": f"_:place_{place}",
                    "dgraph.type": "Place",
                    "name": place,
                    "type": ptype
                })
        if nodes:
            res = txn.mutate(set_obj=nodes)
            txn.commit()
            for key, u in res.uids.items():
                cleaned = key.replace("place_", "")
                uid_map[cleaned] = u
    finally:
        txn.discard()
    return uid_map


def load_devices(file_path, client):
    txn = client.txn()
    uid_map = {}
    try:
        nodes = []
        with open(file_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                alias = row["device_alias"]
                dtype = row["type"]
                status = row["status"]

                nodes.append({
                    "uid": f"_:dev_{alias}",
                    "dgraph.type": "IoTDevice",
                    "name": alias,
                    "type": dtype,
                    "status": status
                })

        if nodes:
            res = txn.mutate(set_obj=nodes)
            txn.commit()
            for key, u in res.uids.items():
                cleaned = key.replace("dev_", "")
                uid_map[cleaned] = u
    finally:
        txn.discard()
    return uid_map

# -------- RELATIONSHIPS FIXED -------- #

def connected_with(file_path, client, device_uids):
    txn = client.txn()
    try:
        with open(file_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                A = row["deviceA"]
                B = row["deviceB"]
                if A in device_uids and B in device_uids:
                    txn.mutate(set_obj={
                        "uid": device_uids[A],
                        "Connected_With": [{"uid": device_uids[B]}]
                    })
        txn.commit()
    finally:
        txn.discard()


def contains_device(file_path, client, place_uids, device_uids):
    txn = client.txn()
    try:
        with open(file_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                place = row["place"]
                device = row["device"]
                if place in place_uids and device in device_uids:
                    txn.mutate(set_obj={
                        "uid": place_uids[place],
                        "Contains_Device": [{"uid": device_uids[device]}]
                    })
        txn.commit()
    finally:
        txn.discard()


def contains_cluster(file_path, client, zone_uids, cluster_uids):
    txn = client.txn()
    try:
        with open(file_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                z = row["zone"]
                c = row["cluster"]
                if z in zone_uids and c in cluster_uids:
                    txn.mutate(set_obj={
                        "uid": zone_uids[z],
                        "Contains_Cluster": [{"uid": cluster_uids[c]}]
                    })
        txn.commit()
    finally:
        txn.discard()


def contains_place(file_path, client, clusters_uids, place_uids):
    txn = client.txn()
    try:
        with open(file_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                z = row["cluster"]
                p = row["place"]
                if z in clusters_uids and p in place_uids:
                    txn.mutate(set_obj={
                        "uid": clusters_uids[z],
                        "Contains_Place": [{"uid": place_uids[p]}]
                    })
        txn.commit()
    finally:
        txn.discard()


def has_brand(file_path, client, device_uids, brand_uids):
    txn = client.txn()
    try:
        with open(file_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                dev = row["device"]
                br = row["brand"]
                if dev in device_uids and br in brand_uids:
                    txn.mutate(set_obj={
                        "uid": device_uids[dev],
                        "Has_Brand": [{"uid": brand_uids[br]}]
                    })
        txn.commit()
    finally:
        txn.discard()


# -------- MASTER LOADER -------- #

def load_data(client):
    print("--- Loading nodes ---")
    brands = load_brands("Dgraph/data/Brand.csv", client)
    zones = load_zones("Dgraph/data/Zone.csv", client)
    clusters = load_clusters("Dgraph/data/Cluster.csv", client)
    places = load_places("Dgraph/data/Place.csv", client)
    devices = load_devices("devices.csv", client)

    print("--- Loading relationships ---")
    contains_cluster("Dgraph/data/Contains_cluster.csv", client, zones, clusters)
    contains_place("Dgraph/data/Contains_place.csv", client, clusters, places)
    contains_device("Dgraph/data/Contains_device.csv", client, places, devices)
    has_brand("Dgraph/data/Has_brand.csv", client, devices, brands)
    connected_with("Dgraph/data/Connected_with.csv", client, devices)

    print("--- DONE ---")

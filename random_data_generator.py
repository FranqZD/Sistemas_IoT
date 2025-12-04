import csv
import random
import datetime
import os


os.makedirs("Cassandra", exist_ok=True)
os.makedirs("Dgraph/data", exist_ok=True)

# Helpers
def random_date():
    today = datetime.date.today()
    delta = datetime.timedelta(days=random.randint(0, 30))
    return (today - delta).isoformat()

def random_tags():
    tags = ["sensor", "iot", "outdoor", "indoor", "critical", "battery"]
    return str(random.sample(tags, random.randint(1, 3)))

def rand_coord():
    return f"({round(random.uniform(-120, -70),4)}, {round(random.uniform(20, 50),4)})"

#Mongo csv

def generate_devices_csv():
    print("Generating devices.csv ...")
    types = ["temperature", "humidity", "smoke", "co2", "motion"]
    models = ["TX-200", "TX-300", "SM-100", "SM-200", "B500", "B900",
              "X100", "X200", "H1", "H2"]
    zones = ["North", "South", "East", "West", "Central"]

    with open("devices.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "device_id", "device_alias", "type", "model", "location",
            "status", "time", "tags",
            "settings_history", "admin_events", "geolocation"
        ])

        for i in range(100):
            writer.writerow([
                f"dev-{i+1}",
                f"alias-{i+1}",
                random.choice(types),
                random.choice(models),
                random.choice(zones),
                random.choice(["ON", "OFF"]),
                random_date(),
                random_tags(),
                "[]",
                "[]",
                rand_coord()
            ])

    print("devices.csv ✓")

def generate_users_csv():
    print("Generating users.csv ...")
    names = ["Valentina", "Carlos", "Andrea", "Miguel", "Sofia", "Ana", "Luis"]
    zones = ["North", "South", "East", "West", "Central"]
    roles = ["admin", "operator", "viewer"]
    types = ["temperature", "humidity", "motion", "co2"]

    with open("users.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "name", "email", "role", "manages_zone", "manages_type"])

        for i in range(20):
            name = random.choice(names)
            writer.writerow([
                f"user-{i+1}",
                name,
                f"{name.lower()}{i+1}@example.com",
                random.choice(roles),
                random.choice(zones),
                random.choice(types)
            ])

    print("users.csv ✓")

def generate_metadata_csv():
    print("Generating metadata.csv ...")
    manufacturers = ["Sony", "Samsung", "Bosch", "Xiaomi", "Honeywell"]
    categories = ["environment", "security", "smoke", "temperature", "energy", "motion"]
    models = ["TX-200", "SM-100", "B500", "X100", "H1"]

    with open("metadata.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["manufacturer", "supported_models", "categories", "country", "website"])

        for m in manufacturers:
            writer.writerow([
                m,
                str(random.sample(models, 2)),
                str(random.sample(categories, 2)),
                "Country",
                f"{m.lower()}.com"
            ])

    print("metadata.csv ✓")

#Cassandra csv

def generate_logs_csv():
    print("Generating logs.csv ...")

    services = ["gateway", "collector", "processor", "notifier"]
    levels = ["INFO", "WARN", "ERROR"]

    with open("Cassandra/logs.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["device_id", "device_alias", "service_name",
                         "level", "log_time", "message", "count"])

        for i in range(200):
            writer.writerow([
                f"dev-{random.randint(1,100)}",
                f"alias-{random.randint(1,100)}",
                random.choice(services),
                random.choice(levels),
                random_date(),
                "Log message",
                random.randint(1,5)
            ])

    print("logs.csv ✓")

def generate_alerts_csv():
    print("Generating alerts.csv ...")

    metrics = ["temp", "humidity", "smoke", "co2", "motion"]
    zones = ["Z1", "Z2", "Z3", "Z4"]

    with open("Cassandra/alerts.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "device_id","device_alias","alert_time",
            "metric_type","threshold_exceeded","zone_id","zone_alias","value"
        ])

        for _ in range(150):
            writer.writerow([
                f"dev-{random.randint(1,100)}",
                f"alias-{random.randint(1,100)}",
                random_date(),
                random.choice(metrics),
                random.choice(["YES","NO"]),
                f"Z{random.randint(1,4)}",
                random.choice(zones),
                round(random.uniform(10, 90), 2)
            ])

    print("alerts.csv ✓")
    
def generate_dgraph_csvs():
    print("Generating Dgraph CSVs ...")

    clusters = ["ClusterA", "ClusterB", "ClusterC"]
    zones = ["North", "South", "East", "West"]
    brands = ["Sony", "Samsung", "Bosch", "Xiaomi", "Honeywell"]
    place_types = ["Office", "Warehouse", "Factory"]
    statuses = ["ON", "OFF"]

    # --- Brand.csv ---
    with open("Dgraph/data/Brand.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "description", "technical_contact"])
        for b in brands:
            writer.writerow([
                b,
                f"{b} description",
                f"support@{b.lower()}.com"
            ])

    # --- Cluster.csv ---
    with open("Dgraph/data/Cluster.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "type", "status", "location"])
        for c in clusters:
            writer.writerow([
                c,
                random.choice(["industrial", "commercial", "mixed"]),
                random.choice(statuses),
                f"Geo({random.uniform(-90,90):.5f},{random.uniform(-180,180):.5f})"
            ])

    # --- Zone.csv ---
    with open("Dgraph/data/Zone.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "location", "description"])
        for z in zones:
            writer.writerow([
                z,
                f"Geo({random.uniform(-90,90):.5f},{random.uniform(-180,180):.5f})",
                f"{z} zone autogenerated"
            ])

    # --- Place.csv ---
    places = [f"Place-{i+1}" for i in range(20)]
    with open("Dgraph/data/Place.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "type", "location"])
        for p in places:
            writer.writerow([
                p,
                random.choice(place_types),
                f"Geo({random.uniform(-90,90):.5f},{random.uniform(-180,180):.5f})"
            ])

    # Relationships
    def rel(filename, col1, col2, list1, list2):
        with open(f"Dgraph/data/{filename}", "w") as f:
            writer = csv.writer(f)
            writer.writerow([col1, col2])
            for _ in range(30):
                writer.writerow([
                    random.choice(list1),
                    random.choice(list2)
                ])

    rel("Connected_with.csv", "deviceA", "deviceB",
        [f"alias-{i}" for i in range(1,20)],
        [f"alias-{i}" for i in range(1,20)])

    rel("Contains_device.csv", "place", "device",
        [f"Place-{i}" for i in range(1,20)],
        [f"alias-{i}" for i in range(1,100)])
 
    rel("Has_brand.csv", "device", "brand",
        [f"alias-{i}" for i in range(1,100)],
        brands)

    rel("Contains_cluster.csv", "zone", "cluster", zones, clusters)
    rel("Contains_place.csv", "cluster", "place", clusters,
        [f"Place-{i}" for i in range(1,20)])

    print("All Dgraph CSVs ✓")


# master
def generate_all_csvs():
    print("\nGenerating all random csv files")
    generate_devices_csv()
    generate_users_csv()
    generate_metadata_csv()
    generate_logs_csv()
    generate_alerts_csv()
    generate_dgraph_csvs()
    print("Csv generation completed")

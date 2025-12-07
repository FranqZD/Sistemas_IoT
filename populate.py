import csv
import ast
from datetime import date
from cassandra.query import BatchStatement

# Función para ejecutar los inserts en lotes (hacer un solo session.execute en lugar de muchos).
def execute_batch(session, stmt, data):
    batch_size = 10
    for i in range(0, len(data), batch_size):
        batch = BatchStatement()
        for item in data[i : i+batch_size]:
            batch.add(stmt, item)
        session.execute(batch)

""" Función para popular tablas:
    readings_by_device_time
    devices_by_type
    devices_by_status_time
"""
def populate_readings(session, file_path):
    # Preparar los statements
    stmt_readings_by_device_time = session.prepare("""
        INSERT INTO readings_by_device_time (
            device_id, device_alias, type, model, location, status, time
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    stmt_devices_by_type = session.prepare("""
        INSERT INTO devices_by_type (
            device_id, device_alias, type, model, location, status, time
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    stmt_devices_by_status_time = session.prepare("""
        INSERT INTO devices_by_status_time (
            device_id, device_alias, type, model, location, status, time
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    readings_data = []
    by_type_data = []
    by_status_time_data = []

    #Leer csv 
    with open(file_path, 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                        device_id = row['device_id']
                        device_alias = row['device_alias']
                        type = row['type']
                        model = row['model']
                        location = row['location']
                        status = row['status']
                        time = date.fromisoformat(row["time"])
                        
                        params = (device_id, device_alias, type, model, location, status, time)
                        #readings_by_device_time
                        readings_data.append(params)
                        #devices_by_type
                        by_type_data.append(params)
                        #devices_by_status_time
                        by_status_time_data.append(params)

    # Ejecutar los statemnts
    execute_batch(session, stmt_readings_by_device_time, readings_data)
    execute_batch(session, stmt_devices_by_type, by_type_data)
    execute_batch(session, stmt_devices_by_status_time, by_status_time_data)

""" Función para popular tablas:
    logs_by_device_time
    logs_by_level_time
    logs_by_service_level_time
    logs_by_service_time
    logs_count_by_device_level_time
"""
def populate_logs(session, file_path):
    # Preparar los statements
    stmt_logs_by_device_time = session.prepare("""
        INSERT INTO logs_by_device_time (
            device_id, device_alias, service_name, level, log_time, message
        ) VALUES (?, ?, ?, ?, ?, ?)
    """)

    stmt_logs_by_level_time = session.prepare("""
        INSERT INTO logs_by_level_time (
            device_id, device_alias, service_name, level, log_time, message
        ) VALUES (?, ?, ?, ?, ?, ?)
    """)

    stmt_logs_by_service_level_time = session.prepare("""
        INSERT INTO logs_by_service_level_time (
            device_id, device_alias, service_name, level, log_time, message
        ) VALUES (?, ?, ?, ?, ?, ?)
    """)

    stmt_logs_by_service_time = session.prepare("""
        INSERT INTO logs_by_service_time (
            device_id, device_alias, service_name, level, log_time, message
        ) VALUES (?, ?, ?, ?, ?, ?)
    """)

    stmt_logs_count_by_device_level_time = session.prepare("""
        INSERT INTO logs_count_by_device_level_time (
            device_id, device_alias, level, log_time, count
        ) VALUES (?, ?, ?, ?, ?)
    """)

    logs_data = []
    by_level_time_data = []
    by_service_level_time_data = []
    by_service_time_data = []
    by_count_by_device_level_time_data = []

    #Leer csv 
    with open(file_path, 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                        device_id = row['device_id']
                        device_alias = row['device_alias']
                        service_name = row['service_name']
                        level = row['level']
                        log_time = date.fromisoformat(row["log_time"])
                        message = row['message']
                        count = int(row['count'])
    
                        params = (device_id, device_alias, service_name, level, log_time, message)
                        #logs_by_device_time
                        logs_data.append(params)
                        #logs_by_level_time
                        by_level_time_data.append(params)
                        #logs_by_service_level_time
                        by_service_level_time_data.append(params)
                        #logs_by_service_time
                        by_service_time_data.append(params)
                        #logs_count_by_device_level_time
                        by_count_by_device_level_time_data.append((device_id, device_alias, level, log_time, count))
                        
    # Ejecutar los statemnts
    execute_batch(session, stmt_logs_by_device_time, logs_data)
    execute_batch(session, stmt_logs_by_level_time, by_level_time_data)
    execute_batch(session, stmt_logs_by_service_level_time, by_service_level_time_data)
    execute_batch(session, stmt_logs_by_service_time, by_service_time_data)
    execute_batch(session, stmt_logs_count_by_device_level_time, by_count_by_device_level_time_data)

""" Función para popular tablas:
    alerts_by_device_time
    alerts_by_metric_time
    alerts_by_zone_time
    alerts_by_metric_value_time
"""
def populate_alerts(session, file_path):
    # Preparar los statements
    stmt_alerts_by_device_time = session.prepare("""
        INSERT INTO alerts_by_device_time (
            device_id, device_alias, alert_time,
            metric_type, threshold_exceeded, zone_id, zone_alias, value
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

    stmt_alerts_by_metric_time = session.prepare("""
        INSERT INTO alerts_by_metric_time (
            device_id, device_alias, alert_time,
            metric_type, threshold_exceeded, zone_id, zone_alias, value
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

    stmt_alerts_by_zone_time = session.prepare("""
        INSERT INTO alerts_by_zone_time (
            device_id, device_alias, alert_time,
            metric_type, threshold_exceeded, zone_id, zone_alias, value
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

    stmt_alerts_by_metric_value_time = session.prepare("""
        INSERT INTO alerts_by_metric_value_time (
            device_id, device_alias, alert_time,
            metric_type, threshold_exceeded, zone_id, zone_alias, value
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

    alerts_data = []
    by_metric_data = []
    by_zone_time_data = []
    by_metric_value_data = []

    #Leer csv 
    with open(file_path, 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                        device_id = row['device_id']
                        device_alias = row['device_alias']
                        alert_time = row['alert_time']
                        metric_type = row['metric_type']
                        threshold_exceeded = row['threshold_exceeded']
                        zone_id = row['zone_id']
                        zone_alias = row['zone_alias']
                        value = float(row['value'])
    
                        params = (device_id, device_alias, alert_time, metric_type, threshold_exceeded, zone_id, zone_alias, value)
                        #alerts_by_device_time
                        alerts_data.append(params)
                        #alerts_by_metric_time
                        by_metric_data.append(params)
                        #alerts_by_zone_time
                        by_zone_time_data.append(params)
                        #alerts_by_metric_value_time
                        by_metric_value_data.append(params)

    # Ejecutar los statemnts
    execute_batch(session, stmt_alerts_by_device_time, alerts_data)
    execute_batch(session, stmt_alerts_by_metric_time, by_metric_data)
    execute_batch(session, stmt_alerts_by_zone_time, by_zone_time_data)
    execute_batch(session, stmt_alerts_by_metric_value_time, by_metric_value_data)

# Mongo section

def load_mongo(db):
    print("\n--- Loading MongoDB Data ---")

    db.devices.delete_many({})
    db.users.delete_many({})
    db.metadata.delete_many({})

    
    # Load devices
    try:
        with open("devices.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            devices = []
            for row in reader:
                devices.append({
                    "device_id": row["device_id"],
                    "device_alias": row["device_alias"],
                    "type": row["type"],
                    "model": row["model"],

                    # Convert location correctly
                    "location": {"zone": row["location"]},

                    "status": row["status"],
                    "time": row["time"],

                    # tags stored as Python list
                    "tags": ast.literal_eval(row["tags"]),

                    # always lists (not strings)
                    "settings_history": [],
                    "admin_events": [],

                    # geolocation optional
                    "geolocation": row.get("geolocation", None)
                })

        if len(devices) > 0:
            db.devices.insert_many(devices)
        print(f"{len(devices)} devices added to Mongo.")

    except Exception as e:
        if "E11000 duplicate key" not in str(e):
            print("Unexpected error loading devices:", e)


    # Load users
    try:
        with open("users.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            users = []
            for row in reader:
                users.append({
                    "user_id": row["user_id"],
                    "name": row["name"],
                    "email": row["email"],
                    "role": row["role"],
                    "manages_zone": row["manages_zone"],
                    "manages_type": row["manages_type"],
                })

        if len(users) > 0:
            db.users.insert_many(users)
        print(f"{len(users)} users added to Mongo.")

    except Exception as e:
        print("Error loading users:", e)

    # Load Metadata
    
    try:
        with open("metadata.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            metadata = []
            for row in reader:
                metadata.append({
                    "manufacturer": row["manufacturer"],
                    "supported_models": ast.literal_eval(row["supported_models"]),
                    "categories": ast.literal_eval(row["categories"]),
                    "country": row["country"],
                    "website": row["website"]
                })

        db.metadata.insert_many(metadata)
        print(f"{len(metadata)} metadata records added.")

    except Exception as e:
        print("Error loading metadata:", e)


    print("\nMongo loading completed.")